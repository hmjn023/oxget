use ratatui::{
    Frame,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, Wrap},
};

use crate::{app::App, model::Status};

pub fn bytes(value: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut n = value as f64;
    let mut unit = 0;
    while n >= 1024.0 && unit + 1 < UNITS.len() {
        n /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{n:.1} {}", UNITS[unit])
    }
}

fn eta(remaining: u64, speed: f64) -> String {
    if speed < 1.0 {
        return "—".into();
    }
    let secs = (remaining as f64 / speed) as u64;
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, secs % 3600 / 60)
    } else {
        format!("{}m {:02}s", secs / 60, secs % 60)
    }
}

fn color(status: Status) -> Color {
    match status {
        Status::Queued => Color::Gray,
        Status::Downloading | Status::Verifying => Color::Cyan,
        Status::Paused => Color::Yellow,
        Status::Done => Color::Green,
        Status::Failed | Status::VerificationFailed => Color::Red,
    }
}

pub fn draw(frame: &mut Frame, app: &mut App) {
    let areas = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(4),
        Constraint::Length(5),
        Constraint::Length(if app.input.is_some() { 3 } else { 2 }),
        Constraint::Length(2),
    ])
    .split(frame.area());
    let completed = app
        .queue
        .jobs
        .iter()
        .filter(|job| job.status == Status::Done)
        .count();
    let speed: f64 = app.active.values().map(|active| active.speed).sum();
    let state = if app.queue.held { "  ALL PAUSED" } else { "" };
    let overall = app.overall();
    let percent = overall["percent"]
        .as_f64()
        .map_or_else(|| "?".into(), |n| format!("{n:.1}%"));
    let remaining = overall["eta_seconds"]
        .as_u64()
        .map_or_else(|| "unknown".into(), |n| eta(n, 1.0));
    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                " oxget ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "  {completed}/{} done   {} active   {}/s   -c {}  -j {}",
                app.queue.jobs.len(),
                app.active.len(),
                bytes(speed as u64),
                app.options.connections,
                app.parallel
            )),
            Span::styled(state, Style::default().fg(Color::Yellow)),
        ]),
        Line::raw(format!(
            " Overall {percent}   {} / {}   ETA {remaining}",
            bytes(overall["downloaded_bytes"].as_u64().unwrap_or(0)),
            overall["total_bytes"]
                .as_u64()
                .map(bytes)
                .unwrap_or_else(|| "unknown".into())
        )),
    ])
    .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, areas[0]);

    let narrow = frame.area().width < 110;
    let rows: Vec<Row> = app
        .queue
        .jobs
        .iter()
        .map(|job| {
            let speed = app.active.get(&job.id).map_or(0.0, |active| active.speed);
            let percent = job.total.map(|size| {
                if size == 0 {
                    if job.status == Status::Done {
                        100.0
                    } else {
                        0.0
                    }
                } else {
                    (job.downloaded as f64 / size as f64 * 100.0).min(100.0)
                }
            });
            let mut cells = vec![
                Cell::from(format!("#{} {}", job.id, job.file_name)),
                Cell::from(job.status.label()).style(Style::default().fg(color(job.status))),
                Cell::from(percent.map_or_else(|| "—".into(), |p| format!("{p:5.1}%"))),
                Cell::from(format!(
                    "{} / {}",
                    bytes(job.downloaded),
                    job.total.map(bytes).unwrap_or_else(|| "?".into())
                )),
                Cell::from(if speed > 0.0 {
                    format!("{}/s", bytes(speed as u64))
                } else {
                    "—".into()
                }),
                Cell::from(
                    job.total
                        .filter(|_| job.status == Status::Downloading)
                        .map_or_else(
                            || "—".into(),
                            |total| eta(total.saturating_sub(job.downloaded), speed),
                        ),
                ),
                Cell::from(if job.connections > 0 {
                    job.connections.to_string()
                } else {
                    "—".into()
                }),
            ];
            if narrow {
                cells.remove(6);
                cells.remove(5);
                cells.remove(3);
            }
            Row::new(cells)
        })
        .collect();
    let widths = if narrow {
        vec![
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(13),
        ]
    } else {
        vec![
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Length(7),
            Constraint::Length(24),
            Constraint::Length(13),
            Constraint::Length(10),
            Constraint::Length(4),
        ]
    };
    let headings = if narrow {
        vec!["ID / File", "Status", "%", "Speed"]
    } else {
        vec![
            "ID / File",
            "Status",
            "%",
            "Downloaded / Size",
            "Speed",
            "ETA",
            "Conn",
        ]
    };
    let table = Table::new(rows, widths)
        .header(
            Row::new(headings)
                .style(Style::default().add_modifier(Modifier::BOLD))
                .bottom_margin(1),
        )
        .block(Block::bordered().title(" Download queue "))
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("› ");
    frame.render_stateful_widget(table, areas[1], &mut app.table);

    if let Some(job) = app.table.selected().and_then(|i| app.queue.jobs.get(i)) {
        let detail =
            Layout::vertical([Constraint::Length(3), Constraint::Length(2)]).split(areas[2]);
        let ratio = job.total.filter(|n| *n > 0).map_or(
            if job.status == Status::Done { 1.0 } else { 0.0 },
            |total| (job.downloaded as f64 / total as f64).clamp(0.0, 1.0),
        );
        frame.render_widget(
            Gauge::default()
                .block(Block::bordered().title(format!(" {} ", job.file_name)))
                .gauge_style(Style::default().fg(color(job.status)))
                .ratio(ratio)
                .label(format!(
                    "{}   {}",
                    job.status.label(),
                    bytes(job.downloaded)
                )),
            detail[0],
        );
        let reason = job.transfer.reason.as_deref().map(|reason| match reason {
            "no_strong_etag" => "No strong ETag: single request, restart from the beginning",
            "range_not_supported" => {
                "Range unsupported: single request, restart from the beginning"
            }
            "range_ignored_during_transfer" => "Server ignored Range: restarted as a full download",
            other => other,
        });
        let text = job.error.as_deref().or(reason).unwrap_or(&job.url);
        frame.render_widget(
            Paragraph::new(text)
                .style(Style::default().fg(if job.error.is_some() {
                    Color::Red
                } else {
                    Color::Gray
                }))
                .wrap(Wrap { trim: true }),
            detail[1],
        );
    } else {
        frame.render_widget(
            Paragraph::new("Your queue is empty. Press a to add a URL or URL list file.")
                .block(Block::bordered()),
            areas[2],
        );
    }
    if let Some(value) = &app.input {
        frame.render_widget(
            Paragraph::new(format!("> {value}▏"))
                .block(
                    Block::bordered().title(" Add URL or list path · Enter confirm · Esc cancel "),
                )
                .style(Style::default().fg(Color::Cyan)),
            areas[3],
        );
    } else {
        frame.render_widget(
            Paragraph::new(app.message.as_str()).wrap(Wrap { trim: true }),
            areas[3],
        );
    }
    frame.render_widget(Paragraph::new("↑/↓ j/k select · Space pause/resume · K/J move up/down · a add\np pause all · r retry failed · q save & quit").style(Style::default().fg(Color::Gray)), areas[4]);
}
