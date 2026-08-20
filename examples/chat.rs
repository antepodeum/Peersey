use std::{collections::HashMap, io::IsTerminal, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use peersey::{Peersey, Room, RoomEvent, RoomKey, Subscription};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph, Wrap},
};
use serde::{Deserialize, Serialize};

const MAX_NAME_CHARS: usize = 32;
const MAX_MESSAGE_CHARS: usize = 4_096;
const WIDE_LAYOUT_MIN_WIDTH: u16 = 92;

#[derive(Debug, Parser)]
#[command(name = "peersey-chat")]
#[command(about = "Private P2P terminal chat over Peersey")]
struct Args {
    /// Your display name (1-32 characters).
    #[arg(value_parser = parse_name)]
    name: String,

    /// Private invite key. Omit to create a new room.
    room: Option<RoomKey>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    name: String,
    text: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum ChatPacket {
    Presence {
        peer: String,
        name: String,
        online_for_ms: u64,
    },
    Message(ChatMessage),
}

#[derive(Debug)]
enum Action {
    None,
    Send(String),
    Quit,
}

#[derive(Debug, Clone)]
enum FeedItem {
    Chat {
        name: String,
        text: String,
        own: bool,
    },
    Presence {
        name: Option<String>,
        peer: String,
        event: PresenceEvent,
    },
    Notice(String),
    Error(String),
}

#[derive(Debug, Clone, Copy)]
enum PresenceEvent {
    Joined,
    ConnectedTo,
    Left,
}

#[derive(Debug, Default)]
struct PeerState {
    name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    Help,
    Invite,
}

struct App {
    name: String,
    local_peer: String,
    room_key: RoomKey,
    created_room: bool,
    started: Instant,
    peers: HashMap<String, PeerState>,
    feed: Vec<FeedItem>,
    input: String,
    cursor: usize,
    scroll_back: usize,
    overlay: Option<Overlay>,
    color: bool,
}

impl App {
    fn new(name: String, room_key: RoomKey, local_peer: String, created_room: bool) -> Self {
        Self {
            name,
            local_peer,
            room_key,
            created_room,
            started: Instant::now(),
            peers: HashMap::new(),
            feed: Vec::new(),
            input: String::new(),
            cursor: 0,
            scroll_back: 0,
            overlay: None,
            color: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    fn handle_event(&mut self, event: Event) -> Action {
        match event {
            Event::Key(key) if key.kind != KeyEventKind::Release => self.handle_key(key),
            Event::Paste(text) if self.overlay.is_none() => {
                self.insert_text(&clean_terminal_text(&text));
                Action::None
            }
            _ => Action::None,
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Action {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('c') => Action::Quit,
                KeyCode::Char('d') if self.input.is_empty() => Action::Quit,
                KeyCode::Char('l') => {
                    self.feed.clear();
                    self.scroll_back = 0;
                    Action::None
                }
                _ => Action::None,
            };
        }

        match key.code {
            KeyCode::F(1) => {
                self.toggle_overlay(Overlay::Help);
                Action::None
            }
            KeyCode::F(2) => {
                self.toggle_overlay(Overlay::Invite);
                Action::None
            }
            KeyCode::Esc if self.overlay.take().is_some() => Action::None,
            _ if self.overlay.is_some() => Action::None,
            KeyCode::Enter => self.submit(),
            KeyCode::Char(character) => {
                self.insert_char(character);
                Action::None
            }
            KeyCode::Backspace => {
                self.backspace();
                Action::None
            }
            KeyCode::Delete => {
                self.delete();
                Action::None
            }
            KeyCode::Left => {
                self.cursor = previous_boundary(&self.input, self.cursor);
                Action::None
            }
            KeyCode::Right => {
                self.cursor = next_boundary(&self.input, self.cursor);
                Action::None
            }
            KeyCode::Home => {
                self.cursor = 0;
                Action::None
            }
            KeyCode::End => {
                self.cursor = self.input.len();
                Action::None
            }
            KeyCode::PageUp => {
                self.scroll_back = self.scroll_back.saturating_add(8).min(self.feed.len());
                Action::None
            }
            KeyCode::PageDown => {
                self.scroll_back = self.scroll_back.saturating_sub(8);
                Action::None
            }
            _ => Action::None,
        }
    }

    fn submit(&mut self) -> Action {
        let text = self.input.trim().to_owned();
        self.input.clear();
        self.cursor = 0;

        if text.is_empty() {
            return Action::None;
        }

        match text.as_str() {
            "/help" => {
                self.overlay = Some(Overlay::Help);
                Action::None
            }
            "/room" => {
                self.overlay = Some(Overlay::Invite);
                Action::None
            }
            "/clear" => {
                self.feed.clear();
                self.scroll_back = 0;
                Action::None
            }
            "/quit" | "/exit" => Action::Quit,
            command if command.starts_with('/') && !command.starts_with("//") => {
                self.push_error(format!("Unknown command: {command} · F1 shows help"));
                Action::None
            }
            message => Action::Send(
                message
                    .strip_prefix("//")
                    .map_or_else(|| message.to_owned(), |rest| format!("/{rest}")),
            ),
        }
    }

    fn insert_char(&mut self, character: char) {
        if !character.is_control() && self.input.chars().count() < MAX_MESSAGE_CHARS {
            self.input.insert(self.cursor, character);
            self.cursor += character.len_utf8();
        }
    }

    fn insert_text(&mut self, text: &str) {
        for character in text.chars() {
            self.insert_char(character);
        }
    }

    fn backspace(&mut self) {
        let previous = previous_boundary(&self.input, self.cursor);
        if previous != self.cursor {
            self.input.drain(previous..self.cursor);
            self.cursor = previous;
        }
    }

    fn delete(&mut self) {
        let next = next_boundary(&self.input, self.cursor);
        if next != self.cursor {
            self.input.drain(self.cursor..next);
        }
    }

    fn toggle_overlay(&mut self, overlay: Overlay) {
        self.overlay = (self.overlay != Some(overlay)).then_some(overlay);
    }

    fn restore_input(&mut self, text: String) {
        self.input = text;
        self.cursor = self.input.len();
    }

    fn push_chat(&mut self, name: impl Into<String>, text: impl Into<String>, own: bool) {
        self.feed.push(FeedItem::Chat {
            name: name.into(),
            text: text.into(),
            own,
        });
        self.scroll_back = 0;
    }

    fn push_notice(&mut self, text: impl Into<String>) {
        self.feed.push(FeedItem::Notice(text.into()));
        self.scroll_back = 0;
    }

    fn push_error(&mut self, text: impl Into<String>) {
        self.feed.push(FeedItem::Error(text.into()));
        self.scroll_back = 0;
    }

    fn on_peer_joined(&mut self, peer: String) {
        self.peers.entry(peer).or_default();
    }

    fn on_presence(&mut self, peer: String, name: String, online_for_ms: u64) {
        if peer == self.local_peer {
            return;
        }

        let name = clean_name(&name);
        let first_announcement = self
            .peers
            .entry(peer.clone())
            .or_default()
            .name
            .replace(name.clone())
            .is_none();
        if first_announcement {
            let event = if online_for_ms < elapsed_ms(self.started) {
                PresenceEvent::Joined
            } else {
                PresenceEvent::ConnectedTo
            };
            self.feed.push(FeedItem::Presence {
                name: Some(name),
                peer,
                event,
            });
        }
    }

    fn on_peer_left(&mut self, peer: String) {
        let name = self.peers.remove(&peer).and_then(|state| state.name);
        self.feed.push(FeedItem::Presence {
            name,
            peer,
            event: PresenceEvent::Left,
        });
        self.scroll_back = 0;
    }

    fn render(&self, frame: &mut Frame) {
        let area = frame.area();
        if area.width < 42 || area.height < 12 {
            frame.render_widget(
                Paragraph::new(
                    "Peersey needs at least 42 × 12\nResize the terminal or press Ctrl+C",
                )
                .alignment(Alignment::Center)
                .style(self.style(Color::Yellow)),
                area,
            );
            return;
        }

        let [header, body, composer, footer] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .areas(area);

        self.render_header(frame, header);
        if area.width >= WIDE_LAYOUT_MIN_WIDTH {
            let [chat, sidebar] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(30)])
                .spacing(1)
                .areas(body);
            self.render_feed(frame, chat);
            self.render_sidebar(frame, sidebar);
        } else {
            self.render_feed(frame, body);
        }
        self.render_composer(frame, composer);
        self.render_footer(frame, footer);

        match self.overlay {
            Some(Overlay::Help) => self.render_help(frame),
            Some(Overlay::Invite) => self.render_invite(frame),
            None => {}
        }
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(self.style(Color::DarkGray));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let [brand, status] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(24)]).areas(inner);
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    " PEERSEY ",
                    self.style(Color::Black)
                        .bg(if self.color {
                            Color::Cyan
                        } else {
                            Color::Reset
                        })
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("  private room", self.style(Color::Gray)),
            ])),
            brand,
        );

        let (label, color) = if self.peers.is_empty() {
            ("○ WAITING", Color::Yellow)
        } else {
            ("● CONNECTED", Color::Green)
        };
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(label, self.style(color).add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!("  {} online ", self.peers.len() + 1),
                    self.style(Color::DarkGray),
                ),
            ]))
            .alignment(Alignment::Right),
            status,
        );
    }

    fn render_feed(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Line::styled(
                " Chat ",
                self.style(Color::Gray).add_modifier(Modifier::BOLD),
            ))
            .title_bottom(Line::styled(
                if self.scroll_back == 0 {
                    " PgUp history "
                } else {
                    " PgDown newest "
                },
                self.style(Color::DarkGray),
            ))
            .border_type(BorderType::Rounded)
            .border_style(self.style(Color::DarkGray))
            .padding(Padding::horizontal(1));
        let inner = block.inner(area);
        let lines = self.feed_lines();
        let max_scroll = lines.len().saturating_sub(inner.height as usize);
        let scroll = max_scroll.saturating_sub(self.scroll_back.min(max_scroll));
        let text = if lines.is_empty() {
            Text::from(vec![
                Line::from(""),
                Line::styled(
                    "Waiting for someone to join",
                    self.style(Color::Gray).add_modifier(Modifier::BOLD),
                ),
                Line::styled(
                    "Press F2 to view the private invite",
                    self.style(Color::DarkGray),
                ),
            ])
        } else {
            Text::from(lines)
        };
        frame.render_widget(
            Paragraph::new(text)
                .block(block)
                .wrap(Wrap { trim: false })
                .scroll((scroll as u16, 0)),
            area,
        );
    }

    fn feed_lines(&self) -> Vec<Line<'static>> {
        self.feed
            .iter()
            .map(|item| match item {
                FeedItem::Chat { name, text, own } => {
                    let name_style = if *own {
                        self.style(Color::Cyan).add_modifier(Modifier::BOLD)
                    } else {
                        self.user_style(name).add_modifier(Modifier::BOLD)
                    };
                    Line::from(vec![
                        Span::styled(format!("{} › ", clean_name(name)), name_style),
                        Span::styled(clean_terminal_text(text), self.style(Color::White)),
                    ])
                }
                FeedItem::Presence { name, peer, event } => {
                    let mut spans = vec![Span::styled("  · ", self.style(Color::DarkGray))];
                    match event {
                        PresenceEvent::ConnectedTo => {
                            spans.push(Span::styled("connected to ", self.style(Color::DarkGray)))
                        }
                        PresenceEvent::Joined | PresenceEvent::Left => {}
                    }
                    if let Some(name) = name {
                        spans.push(Span::styled(
                            clean_name(name),
                            self.user_style(name).add_modifier(Modifier::BOLD),
                        ));
                        spans.push(Span::raw(" "));
                    }
                    spans.push(Span::styled(
                        short_id(peer).to_owned(),
                        self.style(Color::DarkGray),
                    ));
                    let suffix = match event {
                        PresenceEvent::Joined => " joined",
                        PresenceEvent::ConnectedTo => "",
                        PresenceEvent::Left => " left",
                    };
                    spans.push(Span::styled(suffix, self.style(Color::DarkGray)));
                    Line::from(spans)
                }
                FeedItem::Notice(text) => Line::from(vec![
                    Span::styled("  i  ", self.style(Color::Cyan)),
                    Span::styled(clean_terminal_text(text), self.style(Color::Gray)),
                ]),
                FeedItem::Error(text) => Line::from(vec![
                    Span::styled("  !  ", self.style(Color::Red).add_modifier(Modifier::BOLD)),
                    Span::styled(clean_terminal_text(text), self.style(Color::LightRed)),
                ]),
            })
            .collect()
    }

    fn render_sidebar(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Line::styled(
                " Room ",
                self.style(Color::Gray).add_modifier(Modifier::BOLD),
            ))
            .border_type(BorderType::Rounded)
            .border_style(self.style(Color::DarkGray))
            .padding(Padding::horizontal(1));
        let mut lines = vec![
            Line::styled(
                if self.created_room {
                    "CREATED HERE"
                } else {
                    "JOINED"
                },
                self.style(Color::DarkGray),
            ),
            Line::from(""),
            Line::from(vec![
                Span::styled("You  ", self.style(Color::DarkGray)),
                Span::styled(
                    clean_name(&self.name),
                    self.style(Color::Cyan).add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::styled(
                short_id(&self.local_peer).to_owned(),
                self.style(Color::DarkGray),
            ),
            Line::from(""),
            Line::styled("PEOPLE", self.style(Color::DarkGray)),
        ];

        if self.peers.is_empty() {
            lines.push(Line::styled("Waiting…", self.style(Color::Yellow)));
        } else {
            let mut peers: Vec<_> = self.peers.iter().collect();
            peers.sort_by_key(|(peer, state)| (state.name.as_deref().unwrap_or(""), *peer));
            for (peer, state) in peers {
                let name = state.name.as_deref().unwrap_or("connecting…");
                lines.push(Line::from(vec![
                    Span::styled("● ", self.style(Color::Green)),
                    Span::styled(clean_name(name), self.user_style(name)),
                    Span::styled(format!("  {}", short_id(peer)), self.style(Color::DarkGray)),
                ]));
            }
        }

        lines.extend([
            Line::from(""),
            Line::styled("F2  private invite", self.style(Color::Gray)),
            Line::styled("F1  help", self.style(Color::Gray)),
        ]);
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn render_composer(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Line::styled(
                " Message ",
                self.style(Color::Cyan).add_modifier(Modifier::BOLD),
            ))
            .border_type(BorderType::Rounded)
            .border_style(self.style(Color::Cyan))
            .padding(Padding::horizontal(1));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let cursor_width = Line::from(&self.input[..self.cursor]).width();
        let available = inner.width.saturating_sub(1) as usize;
        let horizontal_scroll = cursor_width.saturating_sub(available);
        frame.render_widget(
            Paragraph::new(self.input.as_str())
                .style(self.style(Color::White))
                .scroll((0, horizontal_scroll as u16)),
            inner,
        );
        if self.overlay.is_none() {
            frame.set_cursor_position(Position::new(
                inner.x + (cursor_width - horizontal_scroll) as u16,
                inner.y,
            ));
        }
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    " Enter",
                    self.style(Color::Gray).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" send   ", self.style(Color::DarkGray)),
                Span::styled("F1", self.style(Color::Gray).add_modifier(Modifier::BOLD)),
                Span::styled(" help   ", self.style(Color::DarkGray)),
                Span::styled("F2", self.style(Color::Gray).add_modifier(Modifier::BOLD)),
                Span::styled(" invite   ", self.style(Color::DarkGray)),
                Span::styled(
                    "Ctrl+C",
                    self.style(Color::Gray).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" leave", self.style(Color::DarkGray)),
            ])),
            area,
        );
    }

    fn render_help(&self, frame: &mut Frame) {
        let area = centered_rect(frame.area(), 58, 16);
        frame.render_widget(Clear, area);
        let block = popup_block(" Help ", self.color);
        let lines = vec![
            Line::styled(
                "Keyboard",
                self.style(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Line::from(""),
            Line::from("Enter        send message"),
            Line::from("← → Home End edit message"),
            Line::from("PgUp/PgDown  browse history"),
            Line::from("F2           show private invite"),
            Line::from("Ctrl+L       clear chat view"),
            Line::from("Ctrl+C       leave room"),
            Line::from(""),
            Line::styled(
                "Commands",
                self.style(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Line::from("/room  /clear  /quit  //literal-slash"),
            Line::from(""),
            Line::styled("Esc or F1 closes this panel", self.style(Color::DarkGray)),
        ];
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn render_invite(&self, frame: &mut Frame) {
        let area = centered_rect(frame.area(), 74, 11);
        frame.render_widget(Clear, area);
        let block = popup_block(" Private invite ", self.color);
        let text = Text::from(vec![
            Line::styled(
                "Share only with people you trust.",
                self.style(Color::Yellow).add_modifier(Modifier::BOLD),
            ),
            Line::from(""),
            Line::styled(self.room_key.to_string(), self.style(Color::Cyan)),
            Line::from(""),
            Line::styled(
                "Anyone holding this key can join. Shift-drag to select it.",
                self.style(Color::DarkGray),
            ),
            Line::styled("Esc or F2 closes this panel", self.style(Color::DarkGray)),
        ]);
        frame.render_widget(
            Paragraph::new(text).block(block).wrap(Wrap { trim: false }),
            area,
        );
    }

    fn style(&self, color: Color) -> Style {
        if self.color {
            Style::new().fg(color)
        } else {
            Style::new()
        }
    }

    fn user_style(&self, name: &str) -> Style {
        self.style(user_color(name))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let node = Peersey::new();
    let created_room = args.room.is_none();
    let (room, room_key) = match args.room {
        Some(key) => (node.join_private_room(key).await.context("join room")?, key),
        None => node.create_private_room().await.context("create room")?,
    };
    let events = room.subscribe();
    let app = App::new(
        args.name,
        room_key,
        room.peer_id().to_string(),
        created_room,
    );

    let terminal = ratatui::init();
    let result = run_chat(terminal, app, &room, events).await;
    ratatui::restore();

    room.shutdown().await;
    node.shutdown().await.context("shutdown Peersey")?;
    result
}

async fn run_chat(
    mut terminal: DefaultTerminal,
    mut app: App,
    room: &Room,
    mut room_events: Subscription,
) -> Result<()> {
    let mut terminal_events = EventStream::new();

    loop {
        terminal
            .draw(|frame| app.render(frame))
            .context("draw chat")?;

        tokio::select! {
            terminal_event = terminal_events.next() => {
                let event = terminal_event
                    .context("terminal event stream closed")?
                    .context("read terminal event")?;
                match app.handle_event(event) {
                    Action::None => {}
                    Action::Quit => break,
                    Action::Send(text) => {
                        if app.peers.is_empty() {
                            app.push_notice("Message not sent · waiting for another peer");
                            app.restore_input(text);
                            continue;
                        }
                        let packet = ChatPacket::Message(ChatMessage {
                            name: app.name.clone(),
                            text: text.clone(),
                        });
                        let payload = postcard::to_stdvec(&packet).context("encode chat message")?;
                        match room.send(payload).await {
                            Ok(()) => app.push_chat(app.name.clone(), text, true),
                            Err(error) => {
                                app.restore_input(text);
                                app.push_error(format!("Send failed: {error}"));
                            }
                        }
                    }
                }
            }
            room_event = room_events.recv() => {
                match room_event {
                    Some(RoomEvent::Message { content }) => {
                        match postcard::from_bytes::<ChatPacket>(&content) {
                            Ok(ChatPacket::Message(message)) => {
                                app.push_chat(message.name, clean_terminal_text(&message.text), false);
                            }
                            Ok(ChatPacket::Presence { peer, name, online_for_ms }) => {
                                app.on_presence(peer, name, online_for_ms);
                            }
                            Err(_) => app.push_error("Ignored invalid message"),
                        }
                    }
                    Some(RoomEvent::PeerJoined { peer }) => {
                        app.on_peer_joined(peer.to_string());
                        let presence = ChatPacket::Presence {
                            peer: app.local_peer.clone(),
                            name: app.name.clone(),
                            online_for_ms: elapsed_ms(app.started),
                        };
                        let payload = postcard::to_stdvec(&presence).context("encode presence")?;
                        if let Err(error) = room.send(payload).await {
                            app.push_error(format!("Presence announcement failed: {error}"));
                        }
                    }
                    Some(RoomEvent::PeerLeft { peer }) => app.on_peer_left(peer.to_string()),
                    Some(RoomEvent::Lagged) => app.push_error("Some room events were skipped"),
                    None => {
                        app.push_error("Room connection closed");
                        terminal.draw(|frame| app.render(frame)).context("draw closed room")?;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn popup_block(title: &'static str, color: bool) -> Block<'static> {
    let border_color = if color { Color::Cyan } else { Color::Reset };
    Block::new()
        .title(Line::styled(
            title,
            Style::new().fg(border_color).add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(border_color))
        .padding(Padding::uniform(1))
}

fn centered_rect(area: Rect, max_width: u16, height: u16) -> Rect {
    let width = max_width.min(area.width.saturating_sub(4)).max(1);
    let height = height.min(area.height.saturating_sub(2)).max(1);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn parse_name(value: &str) -> std::result::Result<String, String> {
    let value = value.trim();
    let length = value.chars().count();
    if length == 0 || length > MAX_NAME_CHARS {
        return Err(format!("name must be 1-{MAX_NAME_CHARS} characters"));
    }
    if value.chars().any(char::is_control) {
        return Err("name cannot contain control characters".to_owned());
    }
    Ok(value.to_owned())
}

fn clean_terminal_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character == '\t' {
                ' '
            } else if character.is_control() {
                '�'
            } else {
                character
            }
        })
        .take(MAX_MESSAGE_CHARS)
        .collect()
}

fn clean_name(value: &str) -> String {
    clean_terminal_text(value)
        .chars()
        .take(MAX_NAME_CHARS)
        .collect()
}

fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}

fn previous_boundary(value: &str, cursor: usize) -> usize {
    value[..cursor]
        .char_indices()
        .next_back()
        .map_or(cursor, |(index, _)| index)
}

fn next_boundary(value: &str, cursor: usize) -> usize {
    value[cursor..]
        .char_indices()
        .nth(1)
        .map_or(value.len(), |(index, _)| cursor + index)
}

fn user_color(name: &str) -> Color {
    const COLORS: [Color; 10] = [
        Color::Green,
        Color::Yellow,
        Color::Magenta,
        Color::Cyan,
        Color::LightRed,
        Color::LightGreen,
        Color::LightYellow,
        Color::LightBlue,
        Color::LightMagenta,
        Color::LightCyan,
    ];
    let hash = name
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    COLORS[hash as usize % COLORS.len()]
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{
        Terminal,
        backend::{Backend, TestBackend},
    };

    fn test_app() -> App {
        let mut app = App::new(
            "alice".to_owned(),
            RoomKey::from_bytes([0xab; 32]),
            "0123456789abcdef".to_owned(),
            true,
        );
        app.color = true;
        app
    }

    fn render_text(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        let buffer = terminal.backend().buffer();
        let mut output = String::new();
        for y in 0..height {
            for x in 0..width {
                output.push_str(buffer[(x, y)].symbol());
            }
            output.push('\n');
        }
        output
    }

    #[test]
    fn names_are_trimmed_and_validated() {
        assert_eq!(parse_name("  alice  ").unwrap(), "alice");
        assert!(parse_name(" ").is_err());
        assert!(parse_name(&"x".repeat(MAX_NAME_CHARS + 1)).is_err());
        assert!(parse_name("bad\nname").is_err());
    }

    #[test]
    fn editor_handles_unicode_boundaries() {
        let mut app = test_app();
        app.insert_text("a🙂b");
        app.handle_key(KeyCode::Left.into());
        app.backspace();
        assert_eq!(app.input, "ab");
        assert_eq!(app.cursor, 1);
    }

    #[test]
    fn submit_handles_commands_and_literal_slashes() {
        let mut app = test_app();
        app.restore_input("//hello".to_owned());
        assert!(matches!(app.submit(), Action::Send(text) if text == "/hello"));
        app.restore_input("/help".to_owned());
        assert!(matches!(app.submit(), Action::None));
        assert_eq!(app.overlay, Some(Overlay::Help));
    }

    #[test]
    fn chat_packets_round_trip() {
        let packet = ChatPacket::Presence {
            peer: "peer-id".to_owned(),
            name: "alice".to_owned(),
            online_for_ms: 42,
        };
        let encoded = postcard::to_stdvec(&packet).unwrap();
        assert!(matches!(
            postcard::from_bytes(&encoded).unwrap(),
            ChatPacket::Presence {
                online_for_ms: 42,
                ..
            }
        ));
    }

    #[test]
    fn renders_compact_and_wide_layouts() {
        let mut app = test_app();
        let compact = render_text(&app, 72, 20);
        assert!(compact.contains("PEERSEY"));
        assert!(compact.contains("WAITING"));
        assert!(!compact.contains("CREATED HERE"));

        app.on_peer_joined("fedcba9876543210".to_owned());
        app.on_presence("fedcba9876543210".to_owned(), "bob".to_owned(), 0);
        app.push_chat("bob", "hello", false);
        let wide = render_text(&app, 110, 28);
        assert!(wide.contains("CONNECTED"));
        assert!(wide.contains("CREATED HERE"));
        assert!(wide.contains("bob"));
        assert!(wide.contains("hello"));
    }

    #[test]
    fn backend_trait_stays_available_for_test_backend() {
        fn dimensions<B: Backend>(
            backend: &B,
        ) -> std::result::Result<ratatui::layout::Size, B::Error> {
            backend.size()
        }
        assert_eq!(dimensions(&TestBackend::new(80, 24)).unwrap().width, 80);
    }
}
