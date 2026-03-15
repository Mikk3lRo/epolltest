<?php declare(strict_types = 1);

abstract class abstractListener
{
    public $listenIp = null;
    public $listenPort = null;
    public $numWorkers = null;

    public function __construct($listenIp, $listenPort, $numWorkers = 5)
    {
        $this->listenIp =  $listenIp;
        $this->listenPort =  $listenPort;
        $this->numWorkers =  $numWorkers;
    }


    public function runMaster()
    {
        $serverSocket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($serverSocket === false) {
            throw new RuntimeException("socket_create failed: " . socket_strerror(socket_last_error()));
        }

        socket_set_option($serverSocket, SOL_SOCKET, SO_REUSEADDR, 1);

        socket_set_option($serverSocket, SOL_SOCKET, SO_REUSEPORT, 1);

        if (!socket_bind($serverSocket, $this->listenIp, $this->listenPort)) {
            throw new RuntimeException("socket_bind failed: " . socket_strerror(socket_last_error($serverSocket)));
        }
        if (!socket_listen($serverSocket, 1024)) {
            throw new RuntimeException("socket_listen failed: " . socket_strerror(socket_last_error($serverSocket)));
        }
        socket_set_nonblock($serverSocket);

        // Fork workers before setting up parent signal handlers.
        $childPids = [];
        for ($i = 0; $i < $this->numWorkers; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new RuntimeException("pcntl_fork failed");
            }
            if ($pid === 0) {
                // Child: run worker loop (never returns normally).
                $this->runWorker($serverSocket, $this->listenIp, $this->listenPort);
                exit(0);
            }
            $childPids[] = $pid;
        }

        // Parent no longer needs the listening socket.
        socket_close($serverSocket);

        // Handle SIGINT/SIGTERM
        pcntl_async_signals(true);
        $shutdownRequested = false;
        pcntl_signal(SIGINT,  function () use (&$shutdownRequested) { $shutdownRequested = true; });
        pcntl_signal(SIGTERM, function () use (&$shutdownRequested) { $shutdownRequested = true; });

        fwrite(STDOUT, "[parent] spawned {$this->numWorkers} workers on {$this->listenIp}:{$this->listenPort} — Ctrl+C to stop.\n");

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
        exit(0);
    }

    function runWorker(mixed $serverSock, string $host, int $port): void
    {
        $pid = getmypid();

        pcntl_async_signals(true);

        $stop = false;
        pcntl_signal(SIGTERM, function () use (&$stop) { $stop = true; });
        pcntl_signal(SIGINT, SIG_IGN);

        $base = new EventBase();

        $shutdownTimer = new Event(
            $base,
            -1,
            Event::TIMEOUT | Event::PERSIST,
            function () use (&$stop, $base, $pid) {
                if ($stop) {
                    fwrite(STDOUT, "[worker {$pid}] SIGTERM received, stopping event loop...\n");
                    $base->stop();
                }
            }
        );
        $shutdownTimer->add(0.2); // 200ms tick

        $clients = [];
        $nextId  = 0;

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
                    'id' => $id,
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
                        $this->onData($data, $clients[$id]);
                    }
                );

                $clients[$id]['ev']->add();
            }
        };

        $acceptEvent = new Event($base, $serverSock, Event::READ | Event::PERSIST, $acceptCb);
        $acceptEvent->add();

        fwrite(STDOUT, "[worker {$pid}] ready on {$host}:{$port}\n");

        $base->loop();
        fwrite(STDOUT, "[worker {$pid}] shutting down\n");

        // Clean shutdown: stop accepting, close all open client connections.
        $acceptEvent->del();
        foreach ($clients as $c) {
            $c['ev']->del();
            @socket_close($c['sock']);
        }
        $clients = [];

        fwrite(STDOUT, "[worker {$pid}] exited.\n");
    }


    abstract function onData(string $data, $client);
}
