use std::{io::Result as IoResult, str::FromStr};

use async_chat_server::{ChatHub, Client, RoomInbox, RoomName, RoomPublisher};
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

        if let Some(args) = s.strip_prefix("/leave") {
            if args.starts_with(char::is_whitespace) {
                return Err(ParseInputError::UnexpectedArguments);
            }
        }

        if s == "/switch" {
            return Err(ParseInputError::MissingRoom);
        }

        if let Some(args) = s.strip_prefix("/switch") {
            if args.starts_with(char::is_whitespace) {
                let room = args.trim();
                return if room.is_empty() {
                    Err(ParseInputError::MissingRoom)
                } else {
                    Ok(ClientInput::Switch(RoomName::from(room)))
                };
            }
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

async fn run_joined_session<R, W>(
    reader: R,
    writer: &mut W,
    publisher: RoomPublisher,
    mut inbox: RoomInbox,
) -> IoResult<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();
    loop {
        tokio::select! {
            input = lines.next_line() => {
                match input? {
                    Some(line) => {
                        if let Err(error) = publisher.publish(line) {
                            eprintln!("error publishing message: {error}");
                        }
                    }
                    None => return Ok(()),
                }
            },
            message = inbox.receive() => {
                match message {
                    Some(message) => {
                        writer.write_all(message.to_string().as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                    },
                    None => return Ok(()),
                }
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

    let membership = hub.join(room, client);
    let (publisher, inbox) = membership.split();

    run_joined_session(reader, &mut writer, publisher, inbox).await
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
    use tokio::io::AsyncReadExt;

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
        let (alices_publisher, alices_inbox) = hub.join(room_a.clone(), alice.clone()).split();
        let (_, mut bobs_inbox) = hub.join(room_a, bob.clone()).split();

        run_joined_session(reader, &mut writer, alices_publisher, alices_inbox)
            .await
            .unwrap();

        let first = bobs_inbox.receive().await.unwrap();
        assert!(&first.is_from(&alice));
        assert_eq!(first.text(), "hello");

        let second = bobs_inbox.receive().await.unwrap();
        assert!(&second.is_from(&alice));
        assert_eq!(second.text(), "world");
    }

    #[tokio::test]
    async fn joined_session_writes_messages_with_the_sender() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let (alice_publisher, alice_inbox) = hub.join(room.clone(), Client::from("alice")).split();
        let (bob_publisher, bob_inbox) = hub.join(room, Client::from("bob")).split();
        let (input, session_reader) = tokio::io::duplex(1024);
        let (mut output, session_writer) = tokio::io::duplex(1024);

        let session = tokio::spawn(async move {
            let mut writer = session_writer;
            run_joined_session(
                BufReader::new(session_reader),
                &mut writer,
                alice_publisher,
                alice_inbox,
            )
            .await
        });

        bob_publisher.publish("hello".to_string()).unwrap();

        let mut written = vec![0; "bob: hello\n".len()];
        output.read_exact(&mut written).await.unwrap();

        drop(input);
        session.await.unwrap().unwrap();

        assert_eq!(written, b"bob: hello\n");

        drop(bob_publisher);
        drop(bob_inbox);
        drop(hub);
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
