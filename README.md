# 🚀 ValKeyper

An in-memory key-value store written in Go that replicates core Redis functionalities. Fully compatible with `redis-cli` and supports essential features like GET, SET (with EXPIRE), persistence, replication, streams, and transactions.

## 📋 Features
- **In-memory key-value store**
- **Persistence** to disk
- **Replication** (master-slave)
- **Streams** for event-driven data flows
- **Transactions** for atomic operations 
- **EXPIRE** support for key TTL 

## 🛠️ Installation
```bash
# Clone the repository
git clone https://github.com/vansh845/valkeyper.git
cd valkeyper

# Build the project
go build

# Run the server
./valkeyper
```

## 🚀 Quick Start
```bash
# Use redis-cli to connect
redis-cli -p 6379

# Basic commands
PING
SET key1 "Hello World"
GET key1
EXPIRE key1 10
```

## 🧩 Supported Commands
| Command                  | Description                                    |
|-------------------------|------------------------------------------------|
| **SET key value**        | Set a key to a specific value                  |
| **GET key**              | Get the value of a key                         |
| **EXPIRE key seconds**   | Set a timeout on a key                         |
| **DEL key**              | Delete a key                                   |
| **INCR key**             | Increment the integer value of a key by one    |
| **XADD stream key value**| Add an entry to a stream                       |
| **MULTI**                | Start a transaction                            |
| **EXEC**                 | Execute a transaction                          |

## 📡 Replication Setup
```bash
# Start master server
./valkeyper --port 6379

# Start slave server
./valkeyper --port 6380 --replicaof "127.0.0.1 6379"
```

## 💾 Persistence
Currently it only supports parsing and loading RDB file. Does not take snapshots ( will be added in future).

## 🚦 Transactions
Ensure atomicity of commands by using `MULTI` and `EXEC`.
```bash
redis-cli
> MULTI
> SET balance 100
> INCR balance
> EXEC
```

## 📚 Contributing
Feel free to open issues or submit pull requests! All contributions are welcome.


