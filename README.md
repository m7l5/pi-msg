# pi-session-bridge

Let Pi sessions talk to each other via Unix sockets.

## Install

pi install git:github.com/m7l5/pi-session-bridge

## Usage

/bridge-on [name]       Join the bridge
/bridge-off             Leave the bridge
/bridge-list            List online sessions
/bridge-send <name>     Send a message

## How it works

Sockets at ~/.pi/bridge/<name>.sock.

Online detection = can you connect to the socket?
Offline = socket doesn't exist or refuses connection.

## Roadmap

- [ ] Incoming message notifications in TUI
- [ ] Cold-session wake-up via SDK
- [ ] Named session discovery
