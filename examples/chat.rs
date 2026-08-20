use anyhow::{Context, Result};
use clap::Parser;
use peersey::{Peersey, RoomEvent, RoomKey};
use rustyline::{DefaultEditor, ExternalPrinter, error::ReadlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::time::Instant;
use tokio::sync::mpsc;

const MAX_NAME_CHARS: usize = 32;
const MAX_MESSAGE_CHARS: usize = 4_096;

#[derive(Debug, Parser)]
#[command(name = "peersey-chat")]
#[command(about = "Zero-config P2P chat over Peersey")]
struct Args {
    /// Your display name (1-32 characters).
    #[arg(value_parser = parse_name)]
    name: String,

    /// Secret room key. Omit to create a new private room.
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

enum Input {
    Line(String),
    Quit,
    Error(String),
}

enum Command<'a> {
    Message(&'a str),
    Help,
    Room,
    Clear,
    Quit,
    Unknown(&'a str),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let started = Instant::now();
    let node = Peersey::new();
    let created = args.room.is_none();
    let (room, room_key) = match args.room {
        Some(key) => (node.join_private_room(key).await.context("join room")?, key),
        None => node.create_private_room().await.context("create room")?,
    };

    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    let local_peer = room.peer_id().to_string();
    print_header(&args.name, &room_key, &local_peer, created, color);

    let mut editor = DefaultEditor::new().context("initialize terminal editor")?;
    let mut printer: Box<dyn ExternalPrinter> = match editor.create_external_printer() {
        Ok(printer) => Box::new(printer),
        Err(_) => Box::new(PlainPrinter),
    };
    let prompt = chat_prompt(&args.name, color);
    let (input_tx, mut input_rx) = mpsc::channel(16);
    let input_task = tokio::task::spawn_blocking(move || {
        loop {
            match editor.readline(&prompt) {
                Ok(line) => {
                    if !line.trim().is_empty() {
                        let _ = editor.add_history_entry(line.as_str());
                    }
                    if matches!(line.trim(), "/quit" | "/exit") {
                        let _ = input_tx.blocking_send(Input::Quit);
                        break;
                    }
                    if input_tx.blocking_send(Input::Line(line)).is_err() {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    let _ = input_tx.blocking_send(Input::Quit);
                    break;
                }
                Err(error) => {
                    let _ = input_tx.blocking_send(Input::Error(error.to_string()));
                    break;
                }
            }
        }
    });

    let mut events = room.subscribe();
    let mut connected_peers: HashMap<String, Option<String>> = HashMap::new();

    loop {
        tokio::select! {
            input = input_rx.recv() => {
                match input {
                    Some(Input::Line(line)) => match parse_command(&line) {
                        Command::Message(text) => {
                            if text.is_empty() {
                                continue;
                            }
                            if connected_peers.is_empty() {
                                print_notice(printer.as_mut(), "not sent · waiting for another peer", color);
                                continue;
                            }
                            if text.chars().count() > MAX_MESSAGE_CHARS {
                                print_notice(printer.as_mut(), "message too long (max 4096 characters)", color);
                                continue;
                            }
                            let packet = ChatPacket::Message(ChatMessage {
                                name: args.name.clone(),
                                text: text.to_owned(),
                            });
                            let payload = postcard::to_stdvec(&packet).context("encode chat message")?;
                            room.send(payload).await.context("send chat message")?;
                        }
                        Command::Help => print_help(printer.as_mut(), color),
                        Command::Room => print_room_key(printer.as_mut(), &room_key, color),
                        Command::Clear => clear_screen(printer.as_mut()),
                        Command::Quit => break,
                        Command::Unknown(command) => {
                            print_notice(printer.as_mut(), &format!("unknown command: {command} · try /help"), color);
                        }
                    },
                    Some(Input::Quit) | None => break,
                    Some(Input::Error(error)) => {
                        print_notice(printer.as_mut(), &format!("input error: {error}"), color);
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Some(RoomEvent::Message { content }) => {
                        match postcard::from_bytes::<ChatPacket>(&content) {
                            Ok(ChatPacket::Message(message)) => {
                                print_message(printer.as_mut(), &message.name, &message.text, color);
                            }
                            Ok(ChatPacket::Presence { peer, name, online_for_ms }) => {
                                if peer == local_peer {
                                    continue;
                                }
                                let name = clean_name(&name);
                                let first_announcement = connected_peers
                                    .entry(peer.clone())
                                    .or_default()
                                    .replace(name.clone())
                                    .is_none();
                                if first_announcement {
                                    let arrived_later = online_for_ms < elapsed_ms(started);
                                    print_connected(
                                        printer.as_mut(),
                                        &name,
                                        &peer,
                                        connected_peers.len(),
                                        arrived_later,
                                        color,
                                    );
                                }
                            }
                            Err(_) => print_notice(printer.as_mut(), "ignored invalid message", color),
                        }
                    }
                    Some(RoomEvent::PeerJoined { peer }) => {
                        let peer = peer.to_string();
                        connected_peers.entry(peer).or_default();
                        let presence = ChatPacket::Presence {
                            peer: local_peer.clone(),
                            name: args.name.clone(),
                            online_for_ms: elapsed_ms(started),
                        };
                        let payload = postcard::to_stdvec(&presence).context("encode presence")?;
                        room.send(payload).await.context("announce presence")?;
                    }
                    Some(RoomEvent::PeerLeft { peer }) => {
                        let peer = peer.to_string();
                        let name = connected_peers.remove(&peer).flatten();
                        print_disconnected(
                            printer.as_mut(),
                            name.as_deref(),
                            &peer,
                            connected_peers.len(),
                            color,
                        );
                    }
                    Some(RoomEvent::Lagged) => print_notice(printer.as_mut(), "some messages were skipped", color),
                    None => break,
                }
            }
        }
    }

    print_line(printer.as_mut(), dim("leaving private room…", color));
    room.shutdown().await;
    node.shutdown().await.context("shutdown Peersey")?;
    input_task.await.context("join terminal input task")?;
    Ok(())
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

fn parse_command(line: &str) -> Command<'_> {
    let text = line.trim();
    if text.is_empty() {
        return Command::Message("");
    }
    if text.starts_with("//") {
        return Command::Message(&text[1..]);
    }
    match text {
        "/help" => Command::Help,
        "/room" => Command::Room,
        "/clear" => Command::Clear,
        "/quit" | "/exit" => Command::Quit,
        command if command.starts_with('/') => Command::Unknown(command),
        message => Command::Message(message),
    }
}

fn print_header(name: &str, key: &RoomKey, peer: &str, created: bool, color: bool) {
    println!("{}", bold("peersey · private room", color));
    if created {
        println!(
            "{}",
            dim("new room · share invite only with people you trust", color)
        );
        println!("invite  {key}");
    } else {
        println!("{}", dim("joined with private invite", color));
    }
    println!(
        "you     {} · {}",
        accent(&clean_name(name), color),
        short_id(peer)
    );
    println!("status  {}", accent("waiting for another peer…", color));
    println!(
        "{}",
        dim(
            "Enter send · ↑ history · /help commands · Ctrl+C leave",
            color
        )
    );
    println!();
}

fn print_message(printer: &mut dyn ExternalPrinter, name: &str, text: &str, color: bool) {
    let name = clean_name(name);
    let text = clean_terminal_text(text);
    let label = user_color(&name, color);
    print_line(printer, format!("{label}  {text}"));
}

fn print_connected(
    printer: &mut dyn ExternalPrinter,
    name: &str,
    peer: &str,
    count: usize,
    arrived_later: bool,
    color: bool,
) {
    let identity = peer_identity(name, peer, color);
    let status = if arrived_later {
        format!("{identity} joined")
    } else {
        format!("connected to {identity}")
    };
    print_line(
        printer,
        format!(
            "{} {} {}",
            accent("●", color),
            status,
            dim(&format!("· {count} {} online", peer_word(count)), color)
        ),
    );
}

fn print_disconnected(
    printer: &mut dyn ExternalPrinter,
    name: Option<&str>,
    peer: &str,
    remaining: usize,
    color: bool,
) {
    let identity = name
        .map(|name| peer_identity(name, peer, color))
        .unwrap_or_else(|| dim(short_id(peer), color));
    if remaining == 0 {
        print_line(
            printer,
            format!(
                "{} · {identity} left {}",
                accent("○ waiting", color),
                dim("· no peers connected", color)
            ),
        );
    } else {
        print_line(
            printer,
            format!(
                "· {identity} left {}",
                dim(
                    &format!("· {remaining} {} online", peer_word(remaining)),
                    color
                )
            ),
        );
    }
}

fn print_notice(printer: &mut dyn ExternalPrinter, text: &str, color: bool) {
    print_line(
        printer,
        format!("{} {}", accent("!", color), clean_terminal_text(text)),
    );
}

fn print_help(printer: &mut dyn ExternalPrinter, color: bool) {
    print_line(
        printer,
        format!(
            "{}\n  /room   show private invite\n  /clear  clear screen\n  /quit   leave room\n  //text  send message starting with /",
            bold("commands", color)
        ),
    );
}

fn print_room_key(printer: &mut dyn ExternalPrinter, key: &RoomKey, color: bool) {
    print_line(
        printer,
        format!(
            "{}\n{key}\n{}",
            bold("private invite", color),
            dim("Anyone with this key can join. Share it carefully.", color)
        ),
    );
}

fn clear_screen(printer: &mut dyn ExternalPrinter) {
    let _ = printer.print("\x1b[2J\x1b[H".to_owned());
}

fn print_line(printer: &mut dyn ExternalPrinter, value: String) {
    let _ = printer.print(format!("{value}\n"));
}

struct PlainPrinter;

impl ExternalPrinter for PlainPrinter {
    fn print(&mut self, message: String) -> rustyline::Result<()> {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(message.as_bytes())?;
        stdout.flush()?;
        Ok(())
    }
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

fn chat_prompt(name: &str, color: bool) -> String {
    format!("{} › ", accent(&clean_name(name), color))
}

fn user_color(name: &str, enabled: bool) -> String {
    style(user_color_code(name), name, enabled)
}

fn peer_identity(name: &str, peer: &str, color: bool) -> String {
    format!("{} {}", user_color(name, color), dim(short_id(peer), color))
}

fn user_color_code(name: &str) -> &'static str {
    const COLORS: [&str; 10] = ["32", "33", "35", "36", "91", "92", "93", "94", "95", "96"];
    let hash = name
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    COLORS[hash as usize % COLORS.len()]
}

fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}

fn peer_word(count: usize) -> &'static str {
    if count == 1 { "peer" } else { "peers" }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn bold(value: &str, enabled: bool) -> String {
    style("1", value, enabled)
}

fn dim(value: &str, enabled: bool) -> String {
    style("2", value, enabled)
}

fn accent(value: &str, enabled: bool) -> String {
    style("36", value, enabled)
}

fn style(code: &str, value: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{code}m{value}\x1b[0m")
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_trimmed_and_validated() {
        assert_eq!(parse_name("  alice  ").unwrap(), "alice");
        assert!(parse_name(" ").is_err());
        assert!(parse_name(&"x".repeat(MAX_NAME_CHARS + 1)).is_err());
        assert!(parse_name("bad\nname").is_err());
    }

    #[test]
    fn commands_are_recognized() {
        assert!(matches!(parse_command("/help"), Command::Help));
        assert!(matches!(parse_command("/exit"), Command::Quit));
        assert!(matches!(parse_command("/wat"), Command::Unknown("/wat")));
        assert!(matches!(parse_command("//help"), Command::Message("/help")));
        assert!(matches!(parse_command("hello"), Command::Message("hello")));
    }

    #[test]
    fn terminal_controls_are_removed() {
        assert_eq!(clean_terminal_text("hello\x1b[31m"), "hello�[31m");
        assert_eq!(clean_terminal_text("a\tb"), "a b");
    }

    #[test]
    fn user_colors_are_stable() {
        assert_eq!(user_color_code("alice"), user_color_code("alice"));
        assert!(user_color_code("alice").parse::<u8>().is_ok());
        assert_eq!(chat_prompt("alice", false), "alice › ");
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
}
