# epolltest — PHP 8.4 CLI prefork echo server (PECL `event` / libevent)

A minimal demonstration of a **prefork TCP echo server** in PHP 8.4 CLI using
PECL [`event`](https://pecl.php.net/package/event) (libevent/epoll backend).

## Features

- 5 worker processes, each running its own `EventBase` loop.
- `SO_REUSEPORT` — the kernel distributes incoming connections across workers.
- Non-blocking `socket_accept` + per-connection `Event::READ` handler.
- Tracks connected clients per worker (sequential ID + IP:port) with live log output.
- Reliable shutdown on Ctrl+C / SIGTERM:
  - Parent uses a WNOHANG poll loop (50 ms tick) so signals are processed promptly.
  - Each worker runs a `LOOP_ONCE` tick loop (1 ms idle sleep) so the `$stop` flag
    set by the SIGTERM handler is observed quickly.

---

## Prerequisites — Debian 12 + PHP 8.4

### 1. Add Ondřej Surý's PHP 8.4 repository

```bash
sudo apt install -y lsb-release ca-certificates curl
curl -sSLo /tmp/php-sury.gpg https://packages.sury.org/php/apt.gpg
sudo install -D -o root -g root -m 644 /tmp/php-sury.gpg /etc/apt/keyrings/php-sury.gpg
echo "deb [signed-by=/etc/apt/keyrings/php-sury.gpg] https://packages.sury.org/php/ $(lsb_release -sc) main" \
    | sudo tee /etc/apt/sources.list.d/php-sury.list
sudo apt update
```

### 2. Install PHP 8.4 CLI + required extensions + build tools

```bash
sudo apt install -y \
    php8.4-cli \
    php8.4-sockets \
    php8.4-dev \
    php-pear \
    libevent-dev \
    gcc \
    make \
    pkg-config
```

### 3. Build and install PECL `event`

```bash
sudo pecl install event
```

> **Tip:** if `pecl install event` asks for libevent prefix, answer `/usr`.

### 4. Enable the extensions — load order matters

`ext-event` uses internals from `ext-sockets`, so `sockets` **must** be loaded
first.  On Debian the load order is controlled by the numeric prefix of the
symlinks in `conf.d`.

```bash
# Enable both extensions
sudo phpenmod -v 8.4 sockets

# Create an event.ini that loads *after* sockets (prefix 30 vs 20)
echo "extension=event.so" | sudo tee /etc/php/8.4/mods-available/event.ini
sudo ln -sf /etc/php/8.4/mods-available/event.ini \
            /etc/php/8.4/cli/conf.d/30-event.ini
```

#### Verify the order

```bash
ls -1 /etc/php/8.4/cli/conf.d/ | grep -E 'sockets|event'
# Expected output (sockets number < event number):
#   20-sockets.ini
#   30-event.ini
```

#### Verify both extensions are loaded

```bash
php -m | grep -E '^(sockets|event)$'
php --ri event
```

---

## Running the server

```bash
php testEpoll.php
```

Expected output:

```
[parent] spawned 5 workers on 0.0.0.0:49756 — Ctrl+C to stop.
[worker 12345] ready on 0.0.0.0:49756
[worker 12346] ready on 0.0.0.0:49756
...
```

### Testing with netcat

```bash
# In a second terminal:
nc 127.0.0.1 49756
hello
hello          # echoed back
```

### Testing with multiple clients

```bash
for i in $(seq 1 10); do
    echo "client $i" | nc -q1 127.0.0.1 49756 &
done
wait
```

The server logs each connection and disconnection with the client IP:port and a
running count of connected clients per worker:

```
[worker 12345] +connect #0 127.0.0.1:54321  (clients: 1)
[worker 12346] +connect #0 127.0.0.1:54322  (clients: 1)
[worker 12345] -disconnect #0 127.0.0.1:54321  (clients: 0)
```

### Stopping the server

Press **Ctrl+C** or send SIGTERM to the parent PID.  The parent will forward
SIGTERM to all workers; each worker drains its event loop and closes open
connections before exiting.

---

## Limitations and next steps

| Limitation | Notes |
|---|---|
| No EPOLLOUT / write buffering | `socket_write` is called synchronously in the read callback. Large writes can block a worker. A production implementation should register an `Event::WRITE` handler and buffer partial writes. |
| No backpressure | If a client is slow to read, the write loop will spin. |
| No TLS | libevent supports TLS via `EventBufferEvent`; not implemented here. |
| Fixed port / worker count | Configured at the top of `testEpoll.php`. |

### Possible next steps

1. **EPOLLOUT write buffering** — use `EventBuffer` / `EventBufferEvent` for
   fully non-blocking writes.
2. **Stats command** — add a periodic timer that dumps the connected client list
   per worker to STDOUT.
3. **Graceful drain** — on shutdown, send a `FIN` to all clients and wait for
   them to close before `socket_close`.
4. **Swoole** — for a higher-level abstraction with coroutines and built-in
   connection management, consider [Swoole](https://swoole.com/).
