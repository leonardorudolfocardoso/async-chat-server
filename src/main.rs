use std::{fmt::Display, io::Result};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

type Sender = tokio::sync::broadcast::Sender<Message>;
type Receiver = tokio::sync::broadcast::Receiver<Message>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Name(String);

#[derive(Debug, Clone)]
struct Message {
    from: Name,
    text: String,
}

impl From<String> for Name {
    fn from(value: String) -> Self {
        Name(value.trim().to_owned())
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Message {
    fn new(from: Name, text: impl Into<String>) -> Message {
        Message {
            from,
            text: text.into(),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.from, self.text)
    }
}

async fn ask_name<R, W>(reader: &mut R, writer: &mut W) -> Result<Name>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let msg = b"tell me your name\n";
    writer.write_all(msg).await?;

    let mut name = String::new();
    reader.read_line(&mut name).await?;

    Ok(name.into())
}

async fn greet<W>(writer: &mut W, name: &Name) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let greetings = format!("welcome {name}");
    writer.write_all(greetings.as_bytes()).await
}

async fn write_messages<W>(writer: &mut W, receiver: &mut Receiver, client: &Name) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Ok(msg) = receiver.recv().await {
        if &msg.from != client {
            writer.write_all(msg.to_string().as_bytes()).await?;
        }
    }
    Ok(())
}

fn spawn_message_writer<W>(mut writer: W, sender: Sender, client: Name)
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

async fn propagate_messages<R>(reader: R, sender: Sender, client: Name) -> Result<()>
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

    let name = ask_name(&mut reader, &mut writer).await?;

    greet(&mut writer, &name).await?;

    spawn_message_writer(writer, sender.clone(), name.clone());

    propagate_messages(reader, sender, name).await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn name_from_string_trims_whitespace() {
        let name = Name::from("  alice\n".to_string());

        assert_eq!(name, Name("alice".to_string()));
    }

    #[test]
    fn message_new_preserves_sender_and_text() {
        let name = Name::from("alice".to_string());
        let message = Message::new(name.clone(), "hello");

        assert_eq!(&message.from, &name);
        assert_eq!(message.text, "hello");
    }

    #[tokio::test]
    async fn ask_name_prompts_and_returns_trimmed_name() {
        let mut reader = BufReader::new("  alice\n".as_bytes());
        let mut writer = Vec::new();

        let name = ask_name(&mut reader, &mut writer).await.unwrap();

        assert_eq!(name, Name("alice".to_string()));
        assert_eq!(writer, b"tell me your name\n".to_vec());
    }

    #[tokio::test]
    async fn propagate_messages_broadcasts_each_input_line() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let reader = BufReader::new("hello\nworld\n".as_bytes());
        let client = Name::from("alice".to_string());

        propagate_messages(reader, tx, client.clone())
            .await
            .unwrap();

        let first = rx.recv().await.unwrap();
        assert_eq!(&first.from, &client);
        assert_eq!(first.text, "hello");

        let second = rx.recv().await.unwrap();
        assert_eq!(&second.from, &client);
        assert_eq!(second.text, "world");
    }

    #[tokio::test]
    async fn write_messages_writes_other_clients_only() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let (mut output, mut writer) = tokio::io::duplex(1024);
        let client = Name::from("alice".to_string());
        let writer_client = client.clone();

        let write_task =
            tokio::spawn(async move { write_messages(&mut writer, &mut rx, &writer_client).await });

        tx.send(Message::new(client, "own message")).unwrap();

        let other_message = Message::new(Name::from("bob".to_string()), "hello alice");
        let expected = other_message.to_string();
        tx.send(other_message).unwrap();
        drop(tx);

        write_task.await.unwrap().unwrap();

        let mut written = String::new();
        output.read_to_string(&mut written).await.unwrap();

        assert_eq!(written, expected);
    }
}
