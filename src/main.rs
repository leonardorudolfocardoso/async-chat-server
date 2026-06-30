use std::io::Result;

use async_chat_server::{Client, Message};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::broadcast::error::RecvError,
};

type Sender = tokio::sync::broadcast::Sender<Message>;
type Receiver = tokio::sync::broadcast::Receiver<Message>;

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

async fn greet<W>(writer: &mut W, room: &Client, name: &Client) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let greetings = format!("welcome to room {room}, {name}");
    writer.write_all(greetings.as_bytes()).await
}

async fn write_messages<W>(writer: &mut W, receiver: &mut Receiver, client: &Client) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    loop {
        match receiver.recv().await {
            Ok(msg) => {
                if !msg.is_from(client) {
                    writer.write_all(msg.to_string().as_bytes()).await?;
                }
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => break,
        }
    }
    Ok(())
}

fn spawn_message_writer<W>(mut writer: W, sender: Sender, client: Client)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut receiver = sender.subscribe();
        if let Err(e) = write_messages(&mut writer, &mut receiver, &client).await {
            eprintln!("error writing_messages to {client}: {e}");
        }
    });
}

async fn propagate_messages<R>(reader: R, sender: Sender, client: Client) -> Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    while let Some(text) = lines.next_line().await? {
        if let Err(e) = sender.send(Message::new(client.clone(), text)) {
            eprintln!("error sending message {e}");
        }
    }

    Ok(())
}

async fn handle(stream: TcpStream, sender: Sender) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let name = ask("tell me your name\n", &mut reader, &mut writer)
        .await?
        .into();
    let room = ask("tell me your room\n", &mut reader, &mut writer)
        .await?
        .into();
    greet(&mut writer, &room, &name).await?;

    spawn_message_writer(writer, sender.clone(), name.clone());

    propagate_messages(reader, sender, name).await?;

    Ok(())
}

fn bind_addr_from_args(mut args: impl Iterator<Item = String>) -> String {
    args.next().unwrap_or_else(|| DEFAULT_ADDR.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let addr = bind_addr_from_args(std::env::args().skip(1));

    let (tx, _) = tokio::sync::broadcast::channel::<Message>(16);

    match TcpListener::bind(&addr).await {
        // server binded
        Ok(listener) => loop {
            match listener.accept().await {
                // client connected
                Ok((stream, _)) => {
                    let sender = tx.clone();
                    tokio::spawn(async move { handle(stream, sender).await });
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
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let reader = BufReader::new("hello\nworld\n".as_bytes());
        let client = Client::from("alice".to_string());

        propagate_messages(reader, tx, client.clone())
            .await
            .unwrap();

        let first = rx.recv().await.unwrap();
        assert!(&first.is_from(&client));
        assert_eq!(first.text(), "hello");

        let second = rx.recv().await.unwrap();
        assert!(&second.is_from(&client));
        assert_eq!(second.text(), "world");
    }

    #[tokio::test]
    async fn write_messages_writes_other_clients_only() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let (mut output, mut writer) = tokio::io::duplex(1024);
        let client = Client::from("alice".to_string());
        let writer_client = client.clone();

        let write_task =
            tokio::spawn(async move { write_messages(&mut writer, &mut rx, &writer_client).await });

        tx.send(Message::new(client, "own message")).unwrap();

        let other_message = Message::new(Client::from("bob".to_string()), "hello alice");
        let expected = other_message.to_string();
        tx.send(other_message).unwrap();
        drop(tx);

        write_task.await.unwrap().unwrap();

        let mut written = String::new();
        output.read_to_string(&mut written).await.unwrap();

        assert_eq!(written, expected);
    }

    #[tokio::test]
    async fn write_messages_continues_after_lagging() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(1); // limited sized channel
        let (mut output, mut writer) = tokio::io::duplex(1024);
        let client = Client::from("alice".to_string());

        let missed = Message::new(Client::from("bob".to_string()), "missed");
        let overwritten = Message::new(Client::from("bob".to_string()), "latest");
        let expected = overwritten.to_string();
        tx.send(missed).unwrap();
        tx.send(overwritten).unwrap();
        drop(tx);

        write_messages(&mut writer, &mut rx, &client).await.unwrap();
        drop(writer);

        let mut written = String::new();
        output.read_to_string(&mut written).await.unwrap();

        assert_eq!(written, expected);
    }
}
