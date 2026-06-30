use std::fmt::Display;

use crate::Client;

#[derive(Debug, Clone)]
pub struct Message {
    from: Client,
    text: String,
}

impl Message {
    pub fn new(from: Client, text: impl Into<String>) -> Message {
        Message {
            from,
            text: text.into(),
        }
    }

    pub fn is_from(&self, client: &Client) -> bool {
        &self.from == client
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.from, self.text)
    }
}

#[cfg(test)]
mod test {
    use crate::{Client, Message};

    #[test]
    fn message_new_preserves_sender_and_text() {
        let client = Client::from("alice".to_string());
        let message = Message::new(client.clone(), "hello");

        assert_eq!(&message.from, &client);
        assert_eq!(message.text, "hello");
    }
}
