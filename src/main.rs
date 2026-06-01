use std::{fmt::Display, io::Result};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::broadcast::Sender,
};

type Id = String;

#[derive(Debug, Clone)]
struct Message {
    sender: Id,
    text: String,
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}: {}", self.sender, self.text)
    }
}

async fn handle(stream: TcpStream, sender: Sender<Message>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let msg = b"tell me your name\n";
    writer.write_all(msg).await?;

    let mut name = String::new();
    reader.read_line(&mut name).await?;

    let msg = format!("welcome {name}");
    writer.write_all(msg.as_bytes()).await?;

    let writer_sender = sender.clone();
    let writer_name = name.clone();
    tokio::spawn(async move {
        while let Ok(msg) = writer_sender.subscribe().recv().await {
            if msg.sender != writer_name.trim() {
                writer.write_all(msg.to_string().as_bytes()).await.unwrap();
            }
        }
    });

    let mut lines = reader.lines();
    while let Some(msg) = lines.next_line().await? {
        sender
            .send(Message {
                sender: name.trim().to_string(),
                text: msg,
            })
            .unwrap();
    }

    Ok(())
}

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    let addr = "127.0.0.1:8080";

    rt.block_on(async {
        let (tx, _) = tokio::sync::broadcast::channel::<Message>(16);

        match TcpListener::bind(addr).await {
            // server binded
            Ok(listener) => loop {
                match listener.accept().await {
                    // client connected
                    Ok((stream, _)) => {
                        let sender = tx.clone();
                        rt.spawn(async move { handle(stream, sender).await });
                    }
                    Err(e) => eprintln!("{e}"),
                }
            },
            Err(e) => eprintln!("{e}"),
        }
    });

    Ok(())
}
