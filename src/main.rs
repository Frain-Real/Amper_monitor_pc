use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints, VLine};
use serialport::SerialPort;

const CHANNEL_COUNT: usize = 10;
const MAX_HISTORY: usize = 5_000;
const DEFAULT_BAUD_RATE: u32 = 115_200;
const SERIAL_READ_TIMEOUT: Duration = Duration::from_millis(10);
const ACK_TIMEOUT: Duration = Duration::from_millis(700);
const ADC_FRAME_TIMEOUT: Duration = Duration::from_millis(350);
const DEBUG_LOG_LIMIT: usize = 1_500;
const RX_STREAM_LIMIT: usize = 512;

#[derive(Clone, Copy, Debug)]
struct Sample {
    time_s: f64,
    value: f32,
}

struct ChannelState {
    enabled: bool,
    samples: VecDeque<Sample>,
    latest: Option<f32>,
    device_status: String,
}

impl ChannelState {
    fn new(index: usize) -> Self {
        Self {
            // All channels are requested by default. The selection can be changed before
            // connecting; the requested states are sent to the board after connection.
            enabled: true,
            samples: VecDeque::new(),
            latest: None,
            device_status: format!("CH {:02}: ожидание подключения", index + 1),
        }
    }
}

enum WorkerCommand {
    SetChannel { index: usize, enabled: bool },
    Stop,
}

enum WorkerEvent {
    Connected,
    Disconnected(String),
    Sample { index: usize, time_s: f64, value: f32 },
    ChannelAck {
        index: usize,
        applied: bool,
        ok: bool,
        message: String,
    },
    CommunicationError { index: usize, message: String },
    Status(String),
    Trace(String),
    RxByte(u8),
}

struct AdcMonitorApp {
    ports: Vec<String>,
    selected_port: String,
    baud_rate: u32,
    channels: Vec<ChannelState>,
    worker_tx: Option<Sender<WorkerCommand>>,
    events_tx: Sender<WorkerEvent>,
    events_rx: Receiver<WorkerEvent>,
    connected: bool,
    status: String,
    debug_logs: VecDeque<String>,
    rx_stream: VecDeque<u8>,
    debug_started: Instant,
}

impl AdcMonitorApp {
    fn new() -> Self {
        let (events_tx, events_rx) = mpsc::channel();
        let mut app = Self {
            ports: Vec::new(),
            selected_port: String::new(),
            baud_rate: DEFAULT_BAUD_RATE,
            channels: (0..CHANNEL_COUNT).map(ChannelState::new).collect(),
            worker_tx: None,
            events_tx,
            events_rx,
            connected: false,
            status: "Выберите последовательный порт и нажмите «Подключить».".to_owned(),
            debug_logs: VecDeque::new(),
            rx_stream: VecDeque::new(),
            debug_started: Instant::now(),
        };
        app.refresh_ports();
        app
    }

    fn push_debug(&mut self, message: impl Into<String>) {
        let elapsed = self.debug_started.elapsed().as_secs_f64();
        self.debug_logs
            .push_back(format!("[+{elapsed:09.3}s] {}", message.into()));
        while self.debug_logs.len() > DEBUG_LOG_LIMIT {
            self.debug_logs.pop_front();
        }
    }

    fn refresh_ports(&mut self) {
        match serialport::available_ports() {
            Ok(ports) => {
                self.ports = ports.into_iter().map(|port| port.port_name).collect();
                self.ports.sort();
                if self.selected_port.is_empty() {
                    if let Some(first) = self.ports.first() {
                        self.selected_port = first.clone();
                    }
                }
                if self.ports.is_empty() {
                    self.status = "Порты не найдены. Можно указать путь вручную.".to_owned();
                }
            }
            Err(error) => {
                self.ports.clear();
                self.status = format!("Не удалось получить список портов: {error}");
            }
        }
    }

    fn connect(&mut self) {
        if self.selected_port.trim().is_empty() {
            self.status = "Сначала выберите или введите последовательный порт.".to_owned();
            return;
        }

        let port_name = self.selected_port.trim().to_owned();
        let baud = self.baud_rate;
        self.push_debug(format!("Открытие порта {port_name}, скорость {baud}, формат 8N1."));
        let port = serialport::new(&port_name, baud)
            .timeout(SERIAL_READ_TIMEOUT)
            .open();

        let port = match port {
            Ok(port) => port,
            Err(error) => {
                self.status = format!("Не удалось открыть {port_name}: {error}");
                self.push_debug(format!("ОШИБКА открытия порта: {error}"));
                return;
            }
        };

        // Do not clear the input buffer here: early device output is valuable for
        // diagnosis and must remain visible in the RX viewer.

        let requested_states: [bool; CHANNEL_COUNT] =
            std::array::from_fn(|index| self.channels[index].enabled);
        let (command_tx, command_rx) = mpsc::channel();
        let event_tx = self.events_tx.clone();
        let port_name_for_thread = port_name.clone();

        match thread::Builder::new()
            .name("adc-serial-worker".to_owned())
            .spawn(move || {
                serial_worker(
                    port,
                    command_rx,
                    event_tx,
                    requested_states,
                    port_name_for_thread,
                )
            })
        {
            Ok(_worker) => {
                self.worker_tx = Some(command_tx);
                self.connected = false;
                self.status = format!("Подключение к {port_name}…");
                self.push_debug(format!("Порт {port_name} открыт; запускаю обмен с платой."));
                for (index, channel) in self.channels.iter_mut().enumerate() {
                    channel.device_status = format!("CH {:02}: настройка…", index + 1);
                }
            }
            Err(error) => {
                self.status = format!("Не удалось запустить поток последовательного порта: {error}");
                self.push_debug(format!("ОШИБКА запуска serial-потока: {error}"));
            }
        }
    }

    fn disconnect(&mut self) {
        if let Some(command_tx) = self.worker_tx.take() {
            let _ = command_tx.send(WorkerCommand::Stop);
            self.connected = false;
            self.status = "Отключение…".to_owned();
        }
    }

    fn process_events(&mut self) {
        loop {
            let event = match self.events_rx.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            };

            match event {
                WorkerEvent::Connected => {
                    self.connected = true;
                    self.push_debug("Serial-поток запущен.");
                    self.status = "Подключено. Идёт опрос включённых каналов.".to_owned();
                }
                WorkerEvent::Disconnected(message) => {
                    self.push_debug(format!("ОТКЛЮЧЕНИЕ: {message}"));
                    self.connected = false;
                    self.worker_tx = None;
                    self.status = message;
                    for (index, channel) in self.channels.iter_mut().enumerate() {
                        channel.device_status = format!("CH {:02}: не подключено", index + 1);
                    }
                }
                WorkerEvent::Sample {
                    index,
                    time_s,
                    value,
                } => {
                    if let Some(channel) = self.channels.get_mut(index) {
                        if channel.samples.len() >= MAX_HISTORY {
                            channel.samples.pop_front();
                        }
                        channel.samples.push_back(Sample { time_s, value });
                        channel.latest = Some(value);
                    }
                }
                WorkerEvent::ChannelAck {
                    index,
                    applied,
                    ok,
                    message,
                } => {
                    if let Some(channel) = self.channels.get_mut(index) {
                        // `applied` is the last state accepted by the board. If the board
                        // rejected a command, restore that state in the checkbox.
                        channel.enabled = applied;
                        channel.device_status = if ok {
                            format!("Ответ платы: {message}")
                        } else {
                            format!("Ошибка: {message}")
                        };
                    }
                }
                WorkerEvent::CommunicationError { index, message } => {
                    self.push_debug(format!("CH {:02}: {message}", index + 1));
                    self.status = format!("Канал {:02}: {message}", index + 1);
                    if let Some(channel) = self.channels.get_mut(index) {
                        channel.device_status = format!("Нет данных: {message}");
                    }
                }
                WorkerEvent::Status(message) => {
                    self.push_debug(message.clone());
                    self.status = message;
                }
                WorkerEvent::Trace(message) => self.push_debug(message),
                WorkerEvent::RxByte(byte) => {
                    self.rx_stream.push_back(byte);
                    while self.rx_stream.len() > RX_STREAM_LIMIT {
                        self.rx_stream.pop_front();
                    }
                }
            }
        }
    }

    fn show_connection_bar(&mut self, ui: &mut egui::Ui) {
        let has_worker = self.worker_tx.is_some();

        ui.horizontal_wrapped(|ui| {
            ui.label("Последовательный порт:");
            ui.add_enabled_ui(!has_worker, |ui| {
                ui.add(
                    egui::TextEdit::singleline(&mut self.selected_port)
                        .desired_width(180.0)
                        .hint_text("COM3 / /dev/ttyUSB0"),
                );

                ui.menu_button("Найденные порты", |ui| {
                    if self.ports.is_empty() {
                        ui.label("Порты не найдены");
                    } else {
                        for port in &self.ports {
                            if ui
                                .selectable_label(self.selected_port == *port, port)
                                .clicked()
                            {
                                self.selected_port = port.clone();
                                ui.close();
                            }
                        }
                    }
                });

                ui.label("Скорость:");
                egui::ComboBox::from_id_salt("baud_rate")
                    .selected_text(format!("{} бод", self.baud_rate))
                    .show_ui(ui, |ui| {
                        for rate in [9_600, 19_200, 38_400, 57_600, 115_200, 230_400, 460_800] {
                            ui.selectable_value(
                                &mut self.baud_rate,
                                rate,
                                format!("{rate} бод"),
                            );
                        }
                    });

                if ui.button("⟳ Обновить").clicked() {
                    self.refresh_ports();
                }
            });

            if has_worker {
                if ui.button("Отключить").clicked() {
                    self.disconnect();
                }
            } else if ui.button("Подключить").clicked() {
                self.connect();
            }
        });

        ui.horizontal(|ui| {
            let status_color = if self.connected {
                egui::Color32::from_rgb(70, 190, 110)
            } else {
                ui.visuals().weak_text_color()
            };
            ui.label(egui::RichText::new(if self.connected {
                "● Подключено"
            } else if has_worker {
                "● Подключение"
            } else {
                "● Отключено"
            })
            .color(status_color));
            ui.label(&self.status);
        });

        ui.horizontal_wrapped(|ui| {
            ui.label("Масштаб: колесо мыши; перемещение: перетаскивание правой кнопкой;");
            ui.label("двойной щелчок по графику — сброс масштаба.");
            if ui.button("Сбросить историю графиков").clicked() {
                for channel in &mut self.channels {
                    channel.samples.clear();
                }
            }
        });
    }

    fn show_debug_panel(&mut self, ui: &mut egui::Ui) {
        egui::CollapsingHeader::new("Отладка последовательного порта")
            .default_open(true)
            .show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(format!("Принято байт в буфере: {} / {}", self.rx_stream.len(), RX_STREAM_LIMIT));
                    if ui.button("Очистить RX").clicked() {
                        self.rx_stream.clear();
                    }
                    if ui.button("Очистить журнал").clicked() {
                        self.debug_logs.clear();
                    }
                    ui.weak("RX показывает байты, реально прочитанные из COM/tty; TX и тайм-ауты — в журнале.");
                });

                let received: Vec<u8> = self.rx_stream.iter().copied().collect();
                // Display only the latest 64 bytes as four compact 16-byte rows; keep
                // the longer 512-byte rolling buffer internally for more context.
                let display_start = received.len().saturating_sub(64);
                let display_bytes = &received[display_start..];
                let mut rx_text = if display_bytes.is_empty() {
                    "(пока нет входящих байтов)".to_owned()
                } else {
                    display_bytes
                        .chunks(16)
                        .map(|chunk| {
                            let ascii: String = chunk
                                .iter()
                                .map(|byte| if (0x20..=0x7e).contains(byte) { *byte as char } else { '.' })
                                .collect();
                            format!("HEX: {:<47}  ASCII: {ascii}", format_hex(chunk))
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                let logs: Vec<String> = self.debug_logs.iter().cloned().collect();

                ui.columns(2, |columns| {
                    columns[0].label("RX: последние 64 байта (в буфере хранится до 512)");
                    columns[0].add(
                        egui::TextEdit::multiline(&mut rx_text)
                            .font(egui::TextStyle::Monospace)
                            .desired_rows(4)
                            .interactive(false),
                    );

                    columns[1].label("Журнал обмена");
                    egui::ScrollArea::vertical()
                        .id_salt("serial_debug_log")
                        .max_height(105.0)
                        .stick_to_bottom(true)
                        .show(&mut columns[1], |log_ui| {
                            if logs.is_empty() {
                                log_ui.weak("Записей пока нет.");
                            }
                            for line in &logs {
                                log_ui.monospace(line);
                            }
                        });
                });
            });
    }

    fn show_channel_row(
        ui: &mut egui::Ui,
        index: usize,
        channel: &mut ChannelState,
        command_tx: Option<&Sender<WorkerCommand>>,
    ) {
        let row_width = ui.available_width().max(400.0);
        let latest_width = 156.0;
        let plot_width = (row_width - latest_width - 18.0).max(230.0);
        let row_height = 124.0;

        ui.allocate_ui_with_layout(
            egui::vec2(row_width, row_height),
            egui::Layout::left_to_right(egui::Align::Center),
            |row| {
                row.allocate_ui_with_layout(
                    egui::vec2(plot_width, row_height),
                    egui::Layout::top_down(egui::Align::Min),
                    |plot_column| {
                        plot_column.horizontal(|line| {
                            let response = line.checkbox(
                                &mut channel.enabled,
                                format!("Канал {:02}", index + 1),
                            );
                            if response.changed() {
                                if let Some(sender) = command_tx {
                                    if sender
                                        .send(WorkerCommand::SetChannel {
                                            index,
                                            enabled: channel.enabled,
                                        })
                                        .is_ok()
                                    {
                                        channel.device_status = "Команда отправлена…".to_owned();
                                    } else {
                                        channel.device_status = "Нет соединения с потоком порта".to_owned();
                                    }
                                } else {
                                    channel.device_status = "Применится при подключении".to_owned();
                                }
                            }
                            line.label(
                                egui::RichText::new(&channel.device_status)
                                    .small()
                                    .weak(),
                            );
                        });

                        let sample_copy: Vec<Sample> = channel.samples.iter().copied().collect();
                        let points: PlotPoints = sample_copy
                            .iter()
                            .map(|sample| [sample.time_s, f64::from(sample.value)])
                            .collect();
                        let color = channel_color(index);
                        let mut hovered_sample: Option<Sample> = None;
                        let mut pointer_time: Option<f64> = None;

                        let plot_response = Plot::new(format!("adc_plot_{index}"))
                            .width(plot_width)
                            .height(91.0)
                            .allow_zoom(true)
                            .allow_drag(true)
                            .allow_boxed_zoom(true)
                            .allow_double_click_reset(true)
                            .show_x(true)
                            .show_y(true)
                            .show_crosshair(false)
                            .show(plot_column, |plot_ui| {
                                if !points.is_empty() {
                                    plot_ui.line(
                                        Line::new(format!("Канал {:02}", index + 1), points)
                                            .color(color)
                                            .width(1.7)
                                            .allow_hover(false),
                                    );
                                }

                                if let Some(pointer) = plot_ui.pointer_coordinate() {
                                    pointer_time = Some(pointer.x);
                                    plot_ui.vline(
                                        VLine::new("Курсор", pointer.x)
                                            .color(egui::Color32::from_rgb(235, 185, 70))
                                            .width(1.2),
                                    );

                                    hovered_sample = sample_copy
                                        .iter()
                                        .min_by(|a, b| {
                                            (a.time_s - pointer.x)
                                                .abs()
                                                .total_cmp(&(b.time_s - pointer.x).abs())
                                        })
                                        .copied();
                                }
                            });

                        if plot_response.response.hovered() {
                            if let (Some(pointer_time), Some(sample)) =
                                (pointer_time, hovered_sample)
                            {
                                plot_response.response.on_hover_ui(|tooltip| {
                                    tooltip.label(format!("Время: {:.3} с", sample.time_s));
                                    tooltip.label(format!("Значение: {:.7}", sample.value));
                                    if (pointer_time - sample.time_s).abs() > f64::EPSILON {
                                        tooltip.label(format!("Δt: {:.3} с", (pointer_time - sample.time_s).abs()));
                                    }
                                });
                            }
                        }
                    },
                );

                row.separator();
                row.allocate_ui_with_layout(
                    egui::vec2(latest_width, row_height),
                    egui::Layout::top_down(egui::Align::Center),
                    |right| {
                        right.add_space(12.0);
                        right.label(egui::RichText::new("Последнее значение").small().weak());
                        match channel.latest {
                            Some(value) => {
                                right.label(
                                    egui::RichText::new(format!("{value:.7}"))
                                        .size(19.0)
                                        .strong()
                                        .color(channel_color(index)),
                                );
                            }
                            None => {
                                right.label(egui::RichText::new("—").size(23.0).weak());
                            }
                        }
                        right.label(egui::RichText::new(if channel.enabled {
                            "мониторинг включён"
                        } else {
                            "мониторинг выключен"
                        })
                        .small());
                    },
                );
            },
        );
        ui.separator();
    }
}

impl eframe::App for AdcMonitorApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.process_events();

        if self.worker_tx.is_some() {
            ui.ctx().request_repaint_after(Duration::from_millis(33));
        }

        ui.vertical(|ui| {
            ui.heading("ADC Monitor — 10 каналов");
            ui.add_space(4.0);
            self.show_connection_bar(ui);
            ui.separator();
            self.show_debug_panel(ui);
            ui.separator();

            let command_tx = self.worker_tx.as_ref();
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |scroll_ui| {
                    for index in 0..CHANNEL_COUNT {
                        Self::show_channel_row(
                            scroll_ui,
                            index,
                            &mut self.channels[index],
                            command_tx,
                        );
                    }
                });
        });
    }
}

impl Drop for AdcMonitorApp {
    fn drop(&mut self) {
        if let Some(command_tx) = self.worker_tx.take() {
            let _ = command_tx.send(WorkerCommand::Stop);
        }
    }
}

fn channel_color(index: usize) -> egui::Color32 {
    const COLORS: [[u8; 3]; CHANNEL_COUNT] = [
        [75, 170, 255],
        [255, 160, 70],
        [90, 210, 150],
        [220, 120, 240],
        [245, 215, 90],
        [90, 220, 220],
        [255, 115, 130],
        [160, 180, 255],
        [190, 220, 110],
        [245, 170, 210],
    ];
    let rgb = COLORS[index % COLORS.len()];
    egui::Color32::from_rgb(rgb[0], rgb[1], rgb[2])
}

fn serial_worker(
    mut port: Box<dyn SerialPort>,
    command_rx: Receiver<WorkerCommand>,
    event_tx: Sender<WorkerEvent>,
    requested_states: [bool; CHANNEL_COUNT],
    port_name: String,
) {
    let _ = event_tx.send(WorkerEvent::Connected);
    trace(&event_tx, format!("Открыт serial-порт {port_name}; начинаю синхронизацию 10 каналов."));
    let mut enabled = [false; CHANNEL_COUNT];

    // Bring the board's channel state in sync with the GUI before polling ADC data.
    for (index, requested) in requested_states.into_iter().enumerate() {
        match set_channel_on_board(&mut *port, index, requested, &event_tx) {
            Ok(reply) if reply.trim() == "OK" => {
                enabled[index] = requested;
                trace(&event_tx, format!("ACK CH {:02}: OK; состояние {}.", index + 1, if requested { "включён" } else { "выключен" }));
                let _ = event_tx.send(WorkerEvent::ChannelAck {
                    index,
                    applied: requested,
                    ok: true,
                    message: reply,
                });
            }
            Ok(reply) => {
                trace(&event_tx, format!("Плата не подтвердила настройку CH {:02}: {:?}", index + 1, reply));
                let _ = event_tx.send(WorkerEvent::ChannelAck {
                    index,
                    applied: false,
                    ok: false,
                    message: if reply.is_empty() {
                        "пустой ответ платы".to_owned()
                    } else {
                        reply
                    },
                });
            }
            Err(error) if is_timeout(&error) => {
                // A missing ACK is a communication problem, but does not prove that the
                // command was ignored. Keep the requested state and still try GET_ADC.
                enabled[index] = requested;
                trace(&event_tx, format!(
                    "ТАЙМ-АУТ ACK для {}: {error}. Для диагностики продолжаю опрос канала: {}.",
                    if requested { format!("ON_CHANNEL_{:02}", index + 1) } else { format!("OFF_CHANNEL_{:02}", index + 1) },
                    if requested { "да" } else { "нет" }
                ));
                let _ = event_tx.send(WorkerEvent::ChannelAck {
                    index,
                    applied: requested,
                    ok: false,
                    message: format!("нет ACK (тайм-аут); для диагностики состояние сохранено: {}", if requested { "включён" } else { "выключен" }),
                });
            }
            Err(error) => {
                let _ = event_tx.send(WorkerEvent::Disconnected(format!(
                    "Связь с {port_name} потеряна при настройке каналов: {error}"
                )));
                return;
            }
        }
    }

    let session_start = Instant::now();
    let mut next_index = 0usize;
    let mut last_timeout_notice = Instant::now() - Duration::from_secs(3);

    loop {
        // Process GUI commands between every ADC transaction so channel toggles remain
        // responsive and the serial port has exactly one owner.
        loop {
            match command_rx.try_recv() {
                Ok(command) => {
                    if !handle_worker_command(
                        &mut *port,
                        &event_tx,
                        &mut enabled,
                        command,
                        &port_name,
                    ) {
                        return;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    let _ = event_tx.send(WorkerEvent::Disconnected(
                        "Соединение с GUI закрыто.".to_owned(),
                    ));
                    return;
                }
            }
        }

        let mut channel_to_poll = None;
        for offset in 0..CHANNEL_COUNT {
            let candidate = (next_index + offset) % CHANNEL_COUNT;
            if enabled[candidate] {
                channel_to_poll = Some(candidate);
                break;
            }
        }

        let Some(index) = channel_to_poll else {
            match command_rx.recv_timeout(Duration::from_millis(50)) {
                Ok(command) => {
                    if !handle_worker_command(
                        &mut *port,
                        &event_tx,
                        &mut enabled,
                        command,
                        &port_name,
                    ) {
                        return;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = event_tx.send(WorkerEvent::Disconnected(
                        "Соединение с GUI закрыто.".to_owned(),
                    ));
                    return;
                }
            }
            continue;
        };
        next_index = (index + 1) % CHANNEL_COUNT;

        let command = format!("GET_ADC_{:02}", index + 1);
        if let Err(error) = write_command(&mut *port, &command, &event_tx) {
            let _ = event_tx.send(WorkerEvent::Disconnected(format!(
                "Ошибка записи в {port_name}: {error}"
            )));
            return;
        }

        match read_adc_frame(&mut *port, &event_tx) {
            Ok(bytes) => {
                let value = f32::from_le_bytes(bytes);
                if value.is_finite() {
                    trace(&event_tx, format!("ADC CH {:02}: raw=[{}], f32 little-endian={value:.7}", index + 1, format_hex(&bytes)));
                    let _ = event_tx.send(WorkerEvent::Sample {
                        index,
                        time_s: session_start.elapsed().as_secs_f64(),
                        value,
                    });
                } else {
                    let _ = event_tx.send(WorkerEvent::CommunicationError {
                        index,
                        message: "АЦП вернул NaN или бесконечность".to_owned(),
                    });
                }
            }
            Err(error) if is_timeout(&error) => {
                trace(&event_tx, format!("ТАЙМ-АУТ GET_ADC_{:02}: за {} мс не получен полный кадр; {error}", index + 1, ADC_FRAME_TIMEOUT.as_millis()));
                if last_timeout_notice.elapsed() >= Duration::from_secs(2) {
                    let _ = event_tx.send(WorkerEvent::CommunicationError {
                        index,
                        message: format!("тайм-аут ожидания данных: {error}"),
                    });
                    last_timeout_notice = Instant::now();
                }
                // Do not flush the driver input buffer on timeout; late bytes should remain
                // readable and visible in the RX diagnostics.
            }
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                trace(&event_tx, format!("НЕКОРРЕКТНЫЙ КАДР GET_ADC_{:02}: {error}; входной буфер сохранён для диагностики.", index + 1));
                let _ = event_tx.send(WorkerEvent::CommunicationError {
                    index,
                    message: error.to_string(),
                });
            }
            Err(error) => {
                let _ = event_tx.send(WorkerEvent::Disconnected(format!(
                    "Ошибка чтения из {port_name}: {error}"
                )));
                return;
            }
        }
    }
}

fn handle_worker_command(
    port: &mut dyn SerialPort,
    event_tx: &Sender<WorkerEvent>,
    enabled: &mut [bool; CHANNEL_COUNT],
    command: WorkerCommand,
    port_name: &str,
) -> bool {
    match command {
        WorkerCommand::Stop => {
            let _ = event_tx.send(WorkerEvent::Disconnected("Отключено пользователем.".to_owned()));
            false
        }
        WorkerCommand::SetChannel { index, enabled: requested } => {
            if index >= CHANNEL_COUNT {
                return true;
            }
            let previous = enabled[index];
            match set_channel_on_board(port, index, requested, event_tx) {
                Ok(reply) if reply.trim() == "OK" => {
                    enabled[index] = requested;
                    let _ = event_tx.send(WorkerEvent::ChannelAck {
                        index,
                        applied: requested,
                        ok: true,
                        message: reply,
                    });
                    true
                }
                Ok(reply) => {
                    let _ = event_tx.send(WorkerEvent::ChannelAck {
                        index,
                        applied: previous,
                        ok: false,
                        message: if reply.is_empty() {
                            "пустой ответ платы".to_owned()
                        } else {
                            reply
                        },
                    });
                    true
                }
                Err(error) if is_timeout(&error) => {
                    // Keep the user's requested state for diagnostics and test GET_ADC even
                    // if the board did not return the expected OK line.
                    enabled[index] = requested;
                    trace(event_tx, format!("ТАЙМ-АУТ ACK для {}: {error}; запрошенное состояние сохранено.", if requested { format!("ON_CHANNEL_{:02}", index + 1) } else { format!("OFF_CHANNEL_{:02}", index + 1) }));
                    let _ = event_tx.send(WorkerEvent::ChannelAck {
                        index,
                        applied: requested,
                        ok: false,
                        message: format!("тайм-аут ACK; состояние для диагностики сохранено: {}", if requested { "включён" } else { "выключен" }),
                    });
                    true
                }
                Err(error) => {
                    let _ = event_tx.send(WorkerEvent::Disconnected(format!(
                        "Связь с {port_name} потеряна: {error}"
                    )));
                    false
                }
            }
        }
    }
}

fn set_channel_on_board(
    port: &mut dyn SerialPort,
    index: usize,
    enabled: bool,
    event_tx: &Sender<WorkerEvent>,
) -> io::Result<String> {
    let command = if enabled { "ON_CHANNEL" } else { "OFF_CHANNEL" };
    write_command(port, &format!("{command}_{:02}", index + 1), event_tx)?;
    read_text_line(port, ACK_TIMEOUT, event_tx)
}

fn write_command(
    port: &mut dyn SerialPort,
    command: &str,
    event_tx: &Sender<WorkerEvent>,
) -> io::Result<()> {
    let mut bytes = command.as_bytes().to_vec();
    bytes.push(b'\n');
    trace(
        event_tx,
        format!("TX: {} | HEX [{}]", escape_bytes(&bytes), format_hex(&bytes)),
    );
    port.write_all(&bytes)?;
    port.flush()
}

fn read_text_line(
    port: &mut dyn SerialPort,
    timeout: Duration,
    event_tx: &Sender<WorkerEvent>,
) -> io::Result<String> {
    let deadline = Instant::now() + timeout;
    let mut bytes = Vec::with_capacity(32);
    loop {
        let byte = match read_byte_until(port, deadline, event_tx) {
            Ok(byte) => byte,
            Err(error) => {
                if !bytes.is_empty() {
                    trace(event_tx, format!(
                        "RX неполная текстовая строка перед ошибкой: {} | HEX [{}] ({error})",
                        escape_bytes(&bytes), format_hex(&bytes)
                    ));
                }
                return Err(error);
            }
        };
        if byte == b'\n' {
            while bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
            let response = String::from_utf8_lossy(&bytes).trim().to_owned();
            trace(
                event_tx,
                format!("RX строка: {} | HEX [{}]", escape_bytes(&bytes), format_hex(&bytes)),
            );
            return Ok(response);
        }
        bytes.push(byte);
        if bytes.len() > 128 {
            trace(event_tx, format!("RX ошибка: текстовый ответ длиннее 128 байт: HEX [{}]", format_hex(&bytes)));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "слишком длинный ответ настройки канала",
            ));
        }
    }
}

fn read_adc_frame(
    port: &mut dyn SerialPort,
    event_tx: &Sender<WorkerEvent>,
) -> io::Result<[u8; 4]> {
    let deadline = Instant::now() + ADC_FRAME_TIMEOUT;
    let mut bytes = [0u8; 4];
    let mut received: Vec<u8> = Vec::with_capacity(6);
    for byte in &mut bytes {
        match read_byte_until(port, deadline, event_tx) {
            Ok(value) => {
                *byte = value;
                received.push(value);
            }
            Err(error) => {
                trace(event_tx, format!(
                    "RX неполный кадр float: получено {}/4 байта данных, HEX [{}]; {error}",
                    received.len(), format_hex(&received)
                ));
                return Err(error);
            }
        }
    }

    // The protocol is four raw float bytes followed by LF. Also tolerate CRLF.
    let delimiter = match read_byte_until(port, deadline, event_tx) {
        Ok(value) => value,
        Err(error) => {
            trace(event_tx, format!(
                "RX получены 4 байта float, но нет завершающего LF: HEX [{}]; {error}",
                format_hex(&received)
            ));
            return Err(error);
        }
    };
    received.push(delimiter);
    match delimiter {
        b'\n' => {}
        b'\r' => {
            let next = match read_byte_until(port, deadline, event_tx) {
                Ok(value) => value,
                Err(error) => {
                    trace(event_tx, format!("RX кадр завершился CR, но LF не получен: {error}"));
                    return Err(error);
                }
            };
            received.push(next);
            if next != b'\n' {
                trace(event_tx, format!("RX некорректное окончание кадра: HEX [{}]", format_hex(&received)));
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "после CR ожидался LF в конце кадра АЦП",
                ));
            }
        }
        other => {
            trace(event_tx, format!("RX неверный завершающий байт кадра: HEX [{}]", format_hex(&received)));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("неверный завершающий байт кадра АЦП: 0x{other:02X}"),
            ));
        }
    }
    trace(
        event_tx,
        format!("RX ADC кадр: HEX [{}]", format_hex(&received)),
    );
    Ok(bytes)
}

fn read_byte_until(
    port: &mut dyn SerialPort,
    deadline: Instant,
    event_tx: &Sender<WorkerEvent>,
) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    loop {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "истёк тайм-аут чтения из последовательного порта",
            ));
        }

        match port.read(&mut byte) {
            Ok(1) => {
                let _ = event_tx.send(WorkerEvent::RxByte(byte[0]));
                return Ok(byte[0]);
            }
            Ok(0) => thread::yield_now(),
            Ok(_) => unreachable!("read() requested a one-byte buffer"),
            Err(error) if is_timeout(&error) => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "истёк тайм-аут чтения из последовательного порта",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn trace(event_tx: &Sender<WorkerEvent>, message: impl Into<String>) {
    let _ = event_tx.send(WorkerEvent::Trace(message.into()));
}

fn format_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn escape_bytes(bytes: &[u8]) -> String {
    let mut escaped = String::new();
    for byte in bytes {
        match *byte {
            b'\\' => escaped.push_str("\\\\"),
            b'\n' => escaped.push_str("\\n"),
            b'\r' => escaped.push_str("\\r"),
            b'\t' => escaped.push_str("\\t"),
            0x20..=0x7e => escaped.push(*byte as char),
            other => escaped.push_str(&format!("\\x{other:02X}")),
        }
    }
    format!("\"{escaped}\"")
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 1000.0])
            .with_min_inner_size([820.0, 620.0]),
        ..Default::default()
    };

    eframe::run_native(
        "ADC Monitor — 10 каналов",
        options,
        Box::new(|_creation_context| Ok(Box::new(AdcMonitorApp::new()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_frame_decodes_as_little_endian() {
        let bytes = 12.5_f32.to_le_bytes();
        assert_eq!(f32::from_le_bytes(bytes), 12.5_f32);
    }

    #[test]
    fn history_capacity_is_reasonable_for_all_channels() {
        assert_eq!(CHANNEL_COUNT, 10);
        assert!(MAX_HISTORY >= 1_000);
    }

    #[test]
    fn debug_hex_output_is_byte_exact() {
        assert_eq!(format_hex(&[0x00, 0x0A, 0xFF]), "00 0A FF");
    }

    #[test]
    fn debug_text_escapes_newline_and_binary_bytes() {
        assert_eq!(escape_bytes(b"OK\n"), "\"OK\\n\"");
        assert_eq!(escape_bytes(&[0x00, 0xFF]), "\"\\x00\\xFF\"");
    }
}
