# async-chat-server

A small async TCP chat server written in Rust with Tokio.

This is a study project focused on async Rust, TCP I/O, Tokio tasks, and
channel-based message fanout.

The server accepts multiple TCP clients, asks each client for a name and room,
then broadcasts each line of chat input to the other clients in that room.

## Requirements

- Rust
- Cargo

## Run

```sh
cargo run
```

By default, the server listens on:

```text
127.0.0.1:8080
```

Pass a positional address argument to bind somewhere else:

```sh
cargo run -- 0.0.0.0:8080
```

## Connect

Open two or more terminal sessions and connect with `nc`:

```sh
nc 127.0.0.1 8080
```

Each client will be prompted for a name and room:

```text
who are you?
tell me your room
```

After entering a name, type messages and press Enter. Messages are sent to
other connected clients in this format:

```text
alice: hello
```

The sender does not receive their own messages back, and clients in other rooms
do not receive the message.

## Tests

Run the unit tests:

```sh
cargo test
```

Run formatting and lint checks:

```sh
cargo fmt --check
cargo clippy -- -D warnings
```

The current tests cover the domain and protocol helper logic:

- name trimming
- message construction
- room inbox filtering and lag recovery
- bind address argument parsing
- connection prompt handling
- propagation of input lines through a room publisher
- formatted message output

## Design Notes

The TCP application lives in `src/main.rs`; reusable chat behavior lives in the
library modules under `src/`.

Important internal boundaries:

- `Client` owns client-name normalization.
- `Message` represents a chat message from one named client.
- `RoomName` identifies a room.
- `ChatHub` owns the shared room registry.
- `RoomMembership` splits a joined client into publishing and receiving capabilities.
- `RoomPublisher` publishes messages to one room.
- `RoomInbox` receives messages from one room and owns lag recovery and sender filtering.
- `ask` handles connection prompts.
- `propagate_messages` reads client input and publishes messages.
- `write_messages` receives broadcast messages and writes them to a client.
- `handle` wires one TCP connection into the chat flow.

Each room owns a Tokio broadcast channel. Channel behavior is encapsulated by
the room publisher and inbox rather than exposed to connection handling.

### Message Flow

```mermaid
sequenceDiagram
    participant A as Client A
    participant Server
    participant Hub as Chat Hub
    participant Bus as Room Channel
    participant BWriter as Client B Writer Task
    participant B as Client B

    A->>Server: connect
    Server->>A: who are you / tell me your room
    A->>Server: alice
    A->>Server: rust
    Server->>Hub: join(rust, alice)
    Hub-->>Server: room membership
    Server->>A: welcome to room rust, alice
    Server->>Server: spawn writer task

    A->>Server: hello
    Server->>Bus: publish Message(alice, hello)
    Bus->>BWriter: deliver message
    BWriter->>B: alice: hello
```

## Current Limitations

- There is no graceful shutdown handling.
- Client names are not checked for uniqueness.
- Empty names and empty messages are currently allowed.
- There is no persistence, authentication, or transport security.
- Full TCP integration behavior is not covered by tests yet.
- Empty rooms are retained for the lifetime of the server.
