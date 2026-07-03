use std::{fmt::Display, io::Result as IoResult, str::FromStr};

use async_chat_server::{ChatHub, Client, Message, RoomInbox, RoomName, RoomPublisher};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

#[derive(Debug, PartialEq, Eq)]
enum ClientInput {
    Message(String),
    Switch(RoomName),
    Leave,
}

#[derive(Debug, PartialEq, Eq)]
enum ParseInputError {
    MissingRoom,
    Unknown(String),
    EmptyInput,
    UnexpectedArguments,
}

impl Display for ParseInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRoom => write!(f, "ParseInputError: missing room"),
            Self::Unknown(unknown) => write!(f, "ParseInputError: unknown input {unknown}"),
            Self::EmptyInput => write!(f, "ParseInputError: empty input"),
            Self::UnexpectedArguments => write!(f, "ParseInputError: unexpected arguments"),
        }
    }
}

impl FromStr for ClientInput {
    type Err = ParseInputError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParseInputError::EmptyInput);
        }

        if !s.starts_with('/') {
            return Ok(ClientInput::Message(s.to_owned()));
        }

        if s == "/leave" {
            return Ok(ClientInput::Leave);
        }

        if let Some(args) = s.strip_prefix("/leave")
            && args.starts_with(char::is_whitespace)
        {
            return Err(ParseInputError::UnexpectedArguments);
        }

        if s == "/switch" {
            return Err(ParseInputError::MissingRoom);
        }

        if let Some(args) = s.strip_prefix("/switch")
            && args.starts_with(char::is_whitespace)
        {
            let room = args.trim();
            return if room.is_empty() {
                Err(ParseInputError::MissingRoom)
            } else {
                Ok(ClientInput::Switch(RoomName::from(room)))
            };
        }

        Err(ParseInputError::Unknown(s.to_owned()))
    }
}

async fn ask<R, W>(msg: &str, reader: &mut R, writer: &mut W) -> IoResult<String>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    writer.write_all(msg.as_bytes()).await?;

    let mut response = String::new();
    reader.read_line(&mut response).await?;

    Ok(response)
}

async fn greet<W>(writer: &mut W, room: &RoomName, name: &Client) -> IoResult<()>
where
    W: AsyncWrite + Unpin,
{
    let greetings = format!("welcome to room {room}, {name}\n");
    writer.write_all(greetings.as_bytes()).await
}

#[derive(Debug)]
struct JoinedRoom {
    name: RoomName,
    client: Client,
    publisher: RoomPublisher,
    inbox: RoomInbox,
}

impl JoinedRoom {
    fn join(hub: &ChatHub, name: RoomName, client: Client) -> Self {
        let (publisher, inbox) = hub.join(name.clone(), client.clone()).split();

        Self {
            name,
            client,
            publisher,
            inbox,
        }
    }
    fn switch(self, hub: &ChatHub, name: RoomName) -> Self {
        Self::join(hub, name, self.client)
    }
}

enum SessionEvent {
    Input(ClientInput),
    InvalidInput(ParseInputError),
    Disconnected,
    RoomMessage(Option<Message>),
}

fn session_input(line: Option<String>) -> SessionEvent {
    match line {
        Some(line) => match line.parse() {
            Ok(input) => SessionEvent::Input(input),
            Err(error) => SessionEvent::InvalidInput(error),
        },
        None => SessionEvent::Disconnected,
    }
}

async fn run_session<R, W>(
    reader: R,
    writer: &mut W,
    hub: &ChatHub,
    mut room: JoinedRoom,
) -> IoResult<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();
    loop {
        let event = tokio::select! {
            input = lines.next_line() => session_input(input?),
            message = room.inbox.receive() => SessionEvent::RoomMessage(message),
        };

        match event {
            SessionEvent::Input(ClientInput::Message(text)) => {
                if let Err(error) = room.publisher.publish(text) {
                    eprintln!("error publishing message: {error}");
                }
            }

            SessionEvent::Input(ClientInput::Leave) => {
                let response = format!("left room {}\n", room.name);
                writer.write_all(response.as_bytes()).await?;
                return Ok(());
            }

            SessionEvent::Input(ClientInput::Switch(to)) => {
                let response = if room.name == to {
                    format!("already in room {to}\n")
                } else {
                    room = room.switch(hub, to.clone());
                    format!("switched to room {to}\n")
                };

                writer.write_all(response.as_bytes()).await?;
            }

            SessionEvent::InvalidInput(error) => {
                eprintln!("{error}");
                writer.write_all(b"error: invalid input\n").await?;
            }

            SessionEvent::Disconnected => return Ok(()),

            SessionEvent::RoomMessage(Some(message)) => {
                let response = format!("{message}\n");
                writer.write_all(response.as_bytes()).await?;
            }

            SessionEvent::RoomMessage(None) => {
                writer.write_all(b"error: room closed\n").await?;
                return Ok(());
            }
        }
    }
}

async fn handle(stream: TcpStream, hub: ChatHub) -> IoResult<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let client = ask("who are you?\n", &mut reader, &mut writer)
        .await?
        .into();
    let room = ask("tell me your room\n", &mut reader, &mut writer)
        .await?
        .into();
    greet(&mut writer, &room, &client).await?;

    run_session(
        reader,
        &mut writer,
        &hub,
        JoinedRoom::join(&hub, room, client),
    )
    .await
}

fn bind_addr_from_args(mut args: impl Iterator<Item = String>) -> String {
    args.next().unwrap_or_else(|| DEFAULT_ADDR.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> IoResult<()> {
    let addr = bind_addr_from_args(std::env::args().skip(1));

    let hub = ChatHub::new();

    match TcpListener::bind(&addr).await {
        // server binded
        Ok(listener) => loop {
            match listener.accept().await {
                // client connected
                Ok((stream, _)) => {
                    let hub = hub.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle(stream, hub).await {
                            eprintln!("connection error: {error}");
                        }
                    });
                }
                Err(e) => eprintln!("{e}"),
            }
        },
        Err(e) => eprintln!("{e}"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn bind_addr_from_args_uses_default_when_arg_is_missing() {
        let addr = bind_addr_from_args(std::iter::empty());

        assert_eq!(addr, DEFAULT_ADDR);
    }

    #[test]
    fn bind_addr_from_args_uses_first_arg() {
        let addr = bind_addr_from_args(["0.0.0.0:9000".to_string()].into_iter());

        assert_eq!(addr, "0.0.0.0:9000");
    }

    #[tokio::test]
    async fn ask_prompts_and_returns_response() {
        let mut reader = BufReader::new(" some response\n".as_bytes());
        let mut writer = Vec::new();

        let response = ask("a question\n", &mut reader, &mut writer).await.unwrap();

        assert_eq!(response, " some response\n".to_string());
        assert_eq!(writer, b"a question\n".to_vec());
    }

    #[tokio::test]
    async fn joined_session_broadcasts_each_input_line() {
        let hub = ChatHub::new();
        let reader = BufReader::new("hello\nworld\n".as_bytes());
        let mut writer = Vec::new();
        let room_a = RoomName::from("Room A");
        let alice = Client::from("alice");
        let bob = Client::from("bob");
        let alices_joined_room = JoinedRoom::join(&hub, room_a.clone(), alice.clone());
        let mut bobs_joined_room = JoinedRoom::join(&hub, room_a, bob);

        run_session(reader, &mut writer, &hub, alices_joined_room)
            .await
            .unwrap();

        let first = bobs_joined_room.inbox.receive().await.unwrap();
        assert!(&first.is_from(&alice));
        assert_eq!(first.text(), "hello");

        let second = bobs_joined_room.inbox.receive().await.unwrap();
        assert!(&second.is_from(&alice));
        assert_eq!(second.text(), "world");
    }

    #[tokio::test]
    async fn joined_session_writes_messages_with_the_sender() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let alice_joined_room = JoinedRoom::join(&hub, room.clone(), Client::from("alice"));
        let bob_joined_room = JoinedRoom::join(&hub, room, Client::from("bob"));
        let (input, session_reader) = tokio::io::duplex(1024);
        let (mut output, session_writer) = tokio::io::duplex(1024);
        let session_hub = hub.clone();

        let session = tokio::spawn(async move {
            let mut writer = session_writer;
            run_session(
                BufReader::new(session_reader),
                &mut writer,
                &session_hub,
                alice_joined_room,
            )
            .await
        });

        bob_joined_room
            .publisher
            .publish("hello".to_string())
            .unwrap();

        let mut written = vec![0; "bob: hello\n".len()];
        output.read_exact(&mut written).await.unwrap();

        drop(input);
        session.await.unwrap().unwrap();

        assert_eq!(written, b"bob: hello\n");

        drop(bob_joined_room);
        drop(hub);
    }

    #[tokio::test]
    async fn session_leave_acknowledges_the_room_and_ends_the_session() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let joined_room = JoinedRoom::join(&hub, room, Client::from("alice"));
        let reader = BufReader::new("/leave\nmessage after leave\n".as_bytes());
        let mut writer = Vec::new();

        run_session(reader, &mut writer, &hub, joined_room)
            .await
            .unwrap();

        assert_eq!(writer, b"left room Room A\n");
    }

    #[tokio::test]
    async fn invalid_leave_reports_an_error_and_keeps_the_session_active() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let alice = Client::from("alice");
        let alices_joined_room = JoinedRoom::join(&hub, room.clone(), alice.clone());
        let mut bobs_joined_room = JoinedRoom::join(&hub, room, Client::from("bob"));
        let reader = BufReader::new("/leave now\nhello\n".as_bytes());
        let mut writer = Vec::new();

        run_session(reader, &mut writer, &hub, alices_joined_room)
            .await
            .unwrap();

        let message = bobs_joined_room.inbox.receive().await.unwrap();
        assert!(message.is_from(&alice));
        assert_eq!(message.text(), "hello");
        assert_eq!(writer, b"error: invalid input\n");
    }

    #[tokio::test]
    async fn switching_to_the_current_room_is_a_no_op() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let joined_room = JoinedRoom::join(&hub, room, Client::from("alice"));
        let reader = BufReader::new("/switch Room A\n".as_bytes());
        let mut writer = Vec::new();

        run_session(reader, &mut writer, &hub, joined_room)
            .await
            .unwrap();

        assert_eq!(writer, b"already in room Room A\n");
    }

    #[tokio::test]
    async fn switching_replaces_old_room_delivery_with_new_room_delivery() {
        let hub = ChatHub::new();
        let room_a = RoomName::from("Room A");
        let room_b = RoomName::from("Room B");
        let alice_joined_room = JoinedRoom::join(&hub, room_a.clone(), Client::from("alice"));
        let old_room_member = JoinedRoom::join(&hub, room_a, Client::from("old-bob"));
        let new_room_member = JoinedRoom::join(&hub, room_b, Client::from("new-bob"));
        let (mut input, session_reader) = tokio::io::duplex(1024);
        let (mut output, session_writer) = tokio::io::duplex(1024);
        let session_hub = hub.clone();

        let session = tokio::spawn(async move {
            let mut writer = session_writer;
            run_session(
                BufReader::new(session_reader),
                &mut writer,
                &session_hub,
                alice_joined_room,
            )
            .await
        });

        input.write_all(b"/switch Room B\n").await.unwrap();

        let mut acknowledgement = vec![0; "switched to room Room B\n".len()];
        output.read_exact(&mut acknowledgement).await.unwrap();
        assert_eq!(acknowledgement, b"switched to room Room B\n");

        old_room_member
            .publisher
            .publish("old room message".to_string())
            .unwrap();
        new_room_member
            .publisher
            .publish("new room message".to_string())
            .unwrap();

        let mut delivered = vec![0; "new-bob: new room message\n".len()];
        output.read_exact(&mut delivered).await.unwrap();
        assert_eq!(delivered, b"new-bob: new room message\n");

        drop(input);
        session.await.unwrap().unwrap();
    }

    #[test]
    fn client_input_message_from_str() {
        let input = "hello";

        let client_input: ClientInput = input.parse().unwrap();

        assert_eq!(client_input, ClientInput::Message("hello".to_owned()))
    }

    #[test]
    fn client_input_switch_from_str() {
        let input = "/switch rust room";

        let client_input: ClientInput = input.parse().unwrap();

        assert_eq!(
            client_input,
            ClientInput::Switch(RoomName::from("rust room"))
        )
    }

    #[test]
    fn client_input_switch_from_str_missing_room() {
        let input = "/switch";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::MissingRoom)
    }

    #[test]
    fn client_input_switch_with_whitespace_from_str_missing_room() {
        let input = "/switch   ";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::MissingRoom)
    }

    #[test]
    fn client_input_switch_from_str_preserves_internal_spaces() {
        let input = "/switch rust  room";

        let client_input = ClientInput::from_str(input).unwrap();

        assert_eq!(
            client_input,
            ClientInput::Switch(RoomName::from("rust  room"))
        )
    }

    #[test]
    fn client_input_leave_from_str() {
        let input = "/leave";

        let client_input = ClientInput::from_str(input).unwrap();

        assert_eq!(client_input, ClientInput::Leave);
    }

    #[test]
    fn client_input_leave_with_args_from_str() {
        let input = "/leave something";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::UnexpectedArguments);
    }

    #[test]
    fn client_input_unknown_from_str() {
        let input = "/unknown_cmd";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::Unknown(input.to_owned()));
    }

    #[test]
    fn client_input_unknown_slash_from_str() {
        let input = "/";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::Unknown(input.to_owned()));
    }

    #[test]
    fn client_input_empty_from_str() {
        let input = "";

        let client_input = ClientInput::from_str(input).unwrap_err();

        assert_eq!(client_input, ParseInputError::EmptyInput);
    }
}
