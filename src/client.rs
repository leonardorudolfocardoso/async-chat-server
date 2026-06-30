use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Client(String);

impl From<String> for Client {
    fn from(value: String) -> Self {
        Client::from(value.as_str())
    }
}

impl From<&str> for Client {
    fn from(value: &str) -> Self {
        Client(value.trim().to_owned())
    }
}

impl Display for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod test {
    use crate::Client;

    #[test]
    fn client_from_string_trims_whitespace() {
        let name = Client::from("  alice\n");

        assert_eq!(name, Client::from("alice"));
    }
}
