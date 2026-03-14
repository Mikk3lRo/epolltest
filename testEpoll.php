<?php declare(strict_types=1);
if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Run from CLI.\n");
    exit(1);
}
if (!extension_loaded('sockets')) {
    fwrite(STDERR, "Missing ext-sockets. Install via: apt install php8.4-sockets\n");
    exit(1);
}
if (!extension_loaded('event')) {
    fwrite(STDERR, "Missing ext-event. Install via: pecl install event\n");
    fwrite(STDERR, "NOTE: event.ini must load *after* sockets.ini in conf.d (use a higher numeric prefix).\n");
    exit(1);
}

$host       = '0.0.0.0';
$port       = 49756;
$numWorkers = 5;

// Create listening socket in parent so all children inherit the same fd.
$server = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if ($server === false) {
    throw new RuntimeException("socket_create failed: " . socket_strerror(socket_last_error()));
}

socket_set_option($server, SOL_SOCKET, SO_REUSEADDR, 1);

// SO_REUSEPORT lets the kernel load-balance incoming connections across workers.
if (!defined('SO_REUSEPORT')) {
    throw new RuntimeException("SO_REUSEPORT not defined — upgrade to Linux 3.9+ and PHP with sockets.");
}
socket_set_option($server, SOL_SOCKET, SO_REUSEPORT, 1);

if (!socket_bind($server, $host, $port)) {
    throw new RuntimeException("socket_bind failed: " . socket_strerror(socket_last_error($server)));
}
if (!socket_listen($server, 1024)) {
    throw new RuntimeException("socket_listen failed: " . socket_strerror(socket_last_error($server)));
}
socket_set_nonblock($server);

// Fork workers before setting up parent signal handlers.
$childPids = [];
for ($i = 0; $i < $numWorkers; $i++) {
    $pid = pcntl_fork();
    if ($pid === -1) {
        throw new RuntimeException("pcntl_fork failed");
    }
    if ($pid === 0) {
        // Child: run worker loop (never returns normally).
        runWorker($server, $host, $port);
        exit(0);
    }
    $childPids[] = $pid;
}

// Parent no longer needs the listening socket.
socket_close($server);

// Parent: handle SIGINT/SIGTERM with a non-blocking wait loop so signals are
// processed promptly (blocking waitpid would delay signal delivery).
pcntl_async_signals(true);

$shutdownRequested = false;
pcntl_signal(SIGINT,  function () use (&$shutdownRequested) { $shutdownRequested = true; });
pcntl_signal(SIGTERM, function () use (&$shutdownRequested) { $shutdownRequested = true; });

fwrite(STDOUT, "[parent] spawned {$numWorkers} workers on {$host}:{$port} — Ctrl+C to stop.\n");

// Non-blocking wait loop: wake every 50 ms so signal handlers can run.
while (true) {
    if ($shutdownRequested) {
        fwrite(STDOUT, "[parent] shutdown requested — sending SIGTERM to workers...\n");
        foreach ($childPids as $pid) {
            @posix_kill($pid, SIGTERM);
        }
        $shutdownRequested = false; // signal sent; now just drain the wait loop
    }

    $alive = 0;
    foreach ($childPids as $pid) {
        $res = pcntl_waitpid($pid, $status, WNOHANG);
        if ($res === 0) {
            $alive++;
        }
    }

    if ($alive === 0) {
        break;
    }

    usleep(50_000); // 50 ms — fast enough for responsive Ctrl+C
}

fwrite(STDOUT, "[parent] all workers exited.\n");

// ─────────────────────────────────────────────────────────────────────────────

function runWorker(mixed $serverSock, string $host, int $port): void
{
    $pid = getmypid();

    pcntl_async_signals(true);

    $stop = false;
    pcntl_signal(SIGTERM, function () use (&$stop) { $stop = true; });
    pcntl_signal(SIGINT,  function () use (&$stop) { $stop = true; });

    $base    = new EventBase();
    $clients = [];
    $nextId  = 0;

    // Accept callback.
    // IMPORTANT: the Event callback receives the raw int fd — do NOT pass it to
    // socket_accept().  Instead, always call socket_accept($serverSock) so that
    // PHP gets a proper Socket object back.
    $acceptCb = function ($_fd, $what) use (&$clients, &$nextId, $base, $serverSock, $pid) {
        // Drain the accept queue in one shot (non-blocking socket).
        while (true) {
            $conn = @socket_accept($serverSock);
            if ($conn === false) {
                break; // EAGAIN — no more pending connections
            }

            socket_set_nonblock($conn);

            $ip = 'unknown';
            $p  = 0;
            @socket_getpeername($conn, $ip, $p);

            // Use a stable sequential ID as the $clients key.
            // The raw fd int from libevent callbacks must NOT be used as a key
            // because it can be reused by the OS and does not match this array.
            $id   = $nextId++;
            $peer = "{$ip}:{$p}";

            $clients[$id] = [
                'sock' => $conn,
                'peer' => $peer,
                'ev'   => null,
            ];

            fwrite(STDOUT, "[worker {$pid}] +connect #{$id} {$peer}  (clients: " . count($clients) . ")\n");

            // Read callback: capture $id (stable key) via closure, never use the
            // raw fd argument as an index into $clients.
            $clients[$id]['ev'] = new Event(
                $base,
                $conn,
                Event::READ | Event::PERSIST,
                function ($_fd, $what) use (&$clients, $id, $pid) {
                    if (!isset($clients[$id])) {
                        return;
                    }

                    $sock = $clients[$id]['sock'];
                    $peer = $clients[$id]['peer'];

                    $data = @socket_read($sock, 65536, PHP_BINARY_READ);
                    if ($data === '' || $data === false) {
                        // Client disconnected or error.
                        $clients[$id]['ev']->del();
                        @socket_close($sock);
                        unset($clients[$id]);
                        fwrite(STDOUT, "[worker {$pid}] -disconnect #{$id} {$peer}  (clients: " . count($clients) . ")\n");
                        return;
                    }

                    // Echo the data back to the sender.
                    $len = strlen($data);
                    $off = 0;
                    while ($off < $len) {
                        $sent = @socket_write($sock, substr($data, $off));
                        if ($sent === false || $sent === 0) {
                            break;
                        }
                        $off += $sent;
                    }
                }
            );

            $clients[$id]['ev']->add();
        }
    };

    $acceptEvent = new Event($base, $serverSock, Event::READ | Event::PERSIST, $acceptCb);
    $acceptEvent->add();

    fwrite(STDOUT, "[worker {$pid}] ready on {$host}:{$port}\n");

    // LOOP_ONCE tick loop: each call processes one batch of ready events and
    // returns to PHP userland, where the $stop flag set by the signal handler
    // is visible.  The small usleep keeps CPU usage near-zero when idle.
    while (!$stop) {
        $base->loop(EventBase::LOOP_ONCE);
        usleep(1_000); // 1 ms
    }

    // Clean shutdown: stop accepting, close all open client connections.
    $acceptEvent->del();
    foreach ($clients as $c) {
        $c['ev']->del();
        @socket_close($c['sock']);
    }
    $clients = [];

    fwrite(STDOUT, "[worker {$pid}] exited.\n");
}
