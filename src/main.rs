use std::io::Result;

use async_chat_server::{ChatHub, Client, RoomInbox, RoomName, RoomPublisher};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

async fn ask<R, W>(msg: &str, reader: &mut R, writer: &mut W) -> Result<String>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    writer.write_all(msg.as_bytes()).await?;

    let mut response = String::new();
    reader.read_line(&mut response).await?;

    Ok(response)
}

async fn greet<W>(writer: &mut W, room: &RoomName, name: &Client) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let greetings = format!("welcome to room {room}, {name}\n");
    writer.write_all(greetings.as_bytes()).await
}

fn spawn_message_writer<W>(mut writer: W, mut inbox: RoomInbox)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = write_messages(&mut writer, &mut inbox).await {
            eprintln!("error writing messages: {e}");
        }
    });
}

async fn write_messages<W>(writer: &mut W, inbox: &mut RoomInbox) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(message) = inbox.receive().await {
        let text = format!("{}\n", message);
        writer.write_all(text.as_bytes()).await?;
    }

    Ok(())
}

async fn propagate_messages<R>(reader: R, publisher: RoomPublisher) -> Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    while let Some(text) = lines.next_line().await? {
        if let Err(e) = publisher.publish(text) {
            eprintln!("error sending message {e}");
        }
    }

    Ok(())
}

async fn handle(stream: TcpStream, hub: ChatHub) -> Result<()> {
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

    spawn_message_writer(writer, inbox);

    propagate_messages(reader, publisher).await?;

    Ok(())
}

fn bind_addr_from_args(mut args: impl Iterator<Item = String>) -> String {
    args.next().unwrap_or_else(|| DEFAULT_ADDR.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let addr = bind_addr_from_args(std::env::args().skip(1));

    let hub = ChatHub::new();

    match TcpListener::bind(&addr).await {
        // server binded
        Ok(listener) => loop {
            match listener.accept().await {
                // client connected
                Ok((stream, _)) => {
                    let hub = hub.clone();
                    tokio::spawn(async move { handle(stream, hub).await });
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
    async fn propagate_messages_broadcasts_each_input_line() {
        let hub = ChatHub::new();
        let reader = BufReader::new("hello\nworld\n".as_bytes());
        let room_a = RoomName::from("Room A");
        let alice = Client::from("alice");
        let bob = Client::from("bob");
        let (alices_publisher, _) = hub.join(room_a.clone(), alice.clone()).split();
        let (_, mut bobs_inbox) = hub.join(room_a, bob.clone()).split();

        propagate_messages(reader, alices_publisher).await.unwrap();

        let first = bobs_inbox.receive().await.unwrap();
        assert!(&first.is_from(&alice));
        assert_eq!(first.text(), "hello");

        let second = bobs_inbox.receive().await.unwrap();
        assert!(&second.is_from(&alice));
        assert_eq!(second.text(), "world");
    }

    #[tokio::test]
    async fn write_messages_includes_the_sender() {
        let hub = ChatHub::new();
        let room = RoomName::from("Room A");
        let (alice_publisher, mut alice_inbox) =
            hub.join(room.clone(), Client::from("alice")).split();
        let (bob_publisher, bob_inbox) = hub.join(room, Client::from("bob")).split();
        let (mut output, mut writer) = tokio::io::duplex(1024);

        bob_publisher.publish("hello".to_string()).unwrap();
        drop(alice_publisher);
        drop(bob_publisher);
        drop(bob_inbox);
        drop(hub);

        write_messages(&mut writer, &mut alice_inbox).await.unwrap();
        drop(writer);

        let mut written = String::new();
        output.read_to_string(&mut written).await.unwrap();

        assert_eq!(written, "bob: hello\n");
    }
}
