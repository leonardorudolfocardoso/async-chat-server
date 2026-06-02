use std::{fmt::Display, io::Result};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

type Sender = tokio::sync::broadcast::Sender<Message>;
type Receiver = tokio::sync::broadcast::Receiver<Message>;

type Client = String;

#[derive(Debug, Clone)]
struct Message {
    from: Client,
    text: String,
}

impl Message {
    fn new(from: impl Into<String>, text: impl Into<String>) -> Message {
        Message {
            from: from.into(),
            text: text.into(),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}: {}", self.from, self.text)
    }
}

async fn ask_name(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
) -> Result<String> {
    let msg = b"tell me your name\n";
    writer.write_all(msg).await?;

    let mut name = String::new();
    reader.read_line(&mut name).await?;
    let name = name.trim().to_owned();

    Ok(name)
}

async fn greet<W>(writer: &mut W, name: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let greetings = format!("welcome {name}");
    writer.write_all(greetings.as_bytes()).await
}

async fn write_messages<W>(writer: &mut W, receiver: &mut Receiver, client: &str) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Ok(msg) = receiver.recv().await {
        if msg.from != client {
            writer.write_all(msg.to_string().as_bytes()).await?;
        }
    }
    Ok(())
}

fn spawn_message_writer<W>(mut writer: W, sender: Sender, client: String)
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

async fn propagate_messages<R>(reader: R, sender: Sender, client: &str) -> Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    while let Some(text) = lines.next_line().await? {
        if let Err(e) = sender.send(Message::new(client, text)) {
            eprintln!("error sending message {e}");
        }
    }

    Ok(())
}

async fn handle(stream: TcpStream, sender: Sender) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let name = ask_name(&mut reader, &mut writer).await?;

    greet(&mut writer, &name).await?;

    spawn_message_writer(writer, sender.clone(), name.clone());

    propagate_messages(reader, sender, &name).await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:8080";

    let (tx, _) = tokio::sync::broadcast::channel::<Message>(16);

    match TcpListener::bind(addr).await {
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
