use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex},
};

use tokio::sync::broadcast::{self, error::SendError};

mod message;
pub use message::Message;

mod client;
pub use client::Client;

const ROOM_MESSAGE_CAPACITY: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomName(String);

impl From<String> for RoomName {
    fn from(value: String) -> Self {
        RoomName::from(value.as_str())
    }
}

impl From<&str> for RoomName {
    fn from(value: &str) -> Self {
        RoomName(value.trim().to_owned())
    }
}

impl Display for RoomName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
struct Room {
    sender: broadcast::Sender<Message>,
}

impl Room {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(ROOM_MESSAGE_CAPACITY);
        Self { sender }
    }
    fn join(&mut self, client: Client) -> RoomMembership {
        let publisher = RoomPublisher {
            client: client.clone(),
            sender: self.sender.clone(),
        };
        let inbox = RoomInbox {
            client,
            receiver: self.sender.subscribe(),
        };

        RoomMembership { publisher, inbox }
    }
}

#[derive(Debug)]
pub struct RoomMembership {
    publisher: RoomPublisher,
    inbox: RoomInbox,
}

#[derive(Debug, Clone)]
pub struct RoomPublisher {
    client: Client,
    sender: broadcast::Sender<Message>,
}

#[derive(Debug)]
pub struct RoomInbox {
    client: Client,
    receiver: broadcast::Receiver<Message>,
}

type Rooms = HashMap<RoomName, Room>;
type SharedRooms = Arc<Mutex<Rooms>>;

#[derive(Clone, Default)]
pub struct ChatHub {
    rooms: SharedRooms,
}

impl ChatHub {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn join(&self, room: RoomName, client: Client) -> RoomMembership {
        let mut rooms = self.rooms.lock().unwrap();
        let room = rooms.entry(room).or_insert_with(Room::new);

        room.join(client)
    }
}

impl RoomMembership {
    pub fn split(self) -> (RoomPublisher, RoomInbox) {
        (self.publisher, self.inbox)
    }
}

#[derive(Debug)]
pub struct PublishError(SendError<Message>);

impl From<SendError<Message>> for PublishError {
    fn from(value: SendError<Message>) -> Self {
        Self(value)
    }
}

impl Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublishError: {}", self.0)
    }
}

impl RoomPublisher {
    pub fn publish(&self, text: String) -> Result<usize, PublishError> {
        self.sender
            .send(Message::new(self.client.clone(), text))
            .map_err(PublishError::from)
    }
}

impl RoomInbox {
    pub async fn receive(&mut self) -> Option<Message> {
        loop {
            match self.receiver.recv().await {
                Ok(msg) if msg.is_from(&self.client) => continue,
                Ok(msg) => return Some(msg),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn room_inbox_skips_own_messages() {
        let mut room = Room::new();
        let (alice_publisher, mut alice_inbox) = room.join(Client::from("alice")).split();
        let (bob_publisher, _bob_inbox) = room.join(Client::from("bob")).split();

        alice_publisher.publish("own message".to_string()).unwrap();
        bob_publisher.publish("hello alice".to_string()).unwrap();

        let received = alice_inbox.receive().await.unwrap();

        assert!(received.is_from(&Client::from("bob")));
        assert_eq!(received.text(), "hello alice");
    }

    #[tokio::test]
    async fn room_inbox_continues_after_lagging() {
        let (sender, _) = broadcast::channel(1);
        let mut room = Room { sender };
        let (_, mut alice_inbox) = room.join(Client::from("alice")).split();
        let (bob_publisher, _bob_inbox) = room.join(Client::from("bob")).split();

        bob_publisher.publish("missed".to_string()).unwrap();
        bob_publisher.publish("latest".to_string()).unwrap();

        let received = alice_inbox.receive().await.unwrap();

        assert!(received.is_from(&Client::from("bob")));
        assert_eq!(received.text(), "latest");
    }
}
