<?php declare(strict_types=1);
if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Run from CLI.\n");
    exit(1);
}
if (!extension_loaded('event')) {
    fwrite(STDERR, "Missing ext-event. Install via: pecl install event\n");
    exit(1);
}

$host = '0.0.0.0';
$port = 49756;
$workers = 5;

// Create listening socket in parent so children inherit it.
$server = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if ($server === false) {
    throw new RuntimeException("socket_create failed: " . socket_strerror(socket_last_error()));
}

socket_set_option($server, SOL_SOCKET, SO_REUSEADDR, 1);

// SO_REUSEPORT is required for the “all workers accept on same port” approach.
if (!defined('SO_REUSEPORT')) {
    throw new RuntimeException("SO_REUSEPORT not defined in this PHP build; cannot share port across children in the simplest way.");
}
socket_set_option($server, SOL_SOCKET, SO_REUSEPORT, 1);

if (!socket_bind($server, $host, $port)) {
    throw new RuntimeException("socket_bind failed: " . socket_strerror(socket_last_error($server)));
}
if (!socket_listen($server, 1024)) {
    throw new RuntimeException("socket_listen failed: " . socket_strerror(socket_last_error($server)));
}
socket_set_nonblock($server);

// Fork workers
$childPids = [];
for ($i = 0; $i < $workers; $i++) {
    $pid = pcntl_fork();
    if ($pid === -1) {
        throw new RuntimeException("pcntl_fork failed");
    }
    if ($pid === 0) {
        runWorker($server, $host, $port);
        exit(0);
    }
    $childPids[] = $pid;
}

// Parent: terminate children on SIGINT/SIGTERM and wait.
pcntl_async_signals(true);
$shutdown = function() use (&$childPids) {
    echo 'Shutdown!';
    foreach ($childPids as $pid) {
        echo 'Killing ' . $pid . '!';
        @posix_kill($pid, SIGTERM);
    }
    echo "Done\n";
    die();
};
pcntl_signal(SIGINT, $shutdown);
pcntl_signal(SIGTERM, $shutdown);

echo 'waiting';

foreach ($childPids as $pid) {
    pcntl_waitpid($pid, $status);
}
echo 'fin';

function runWorker($serverSock, string $host, int $port): void
{
    $pid = getmypid();

    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function() { EventBase::getDefault()->stop(); });
    pcntl_signal(SIGINT, function() { EventBase::getDefault()->stop(); });

    $base = new EventBase();

    $clients = [];

    // IMPORTANT: callback receives $fd as an int; use $serverSock instead.
    $acceptEvent = new Event(
        $base,
        $serverSock,
        Event::READ | Event::PERSIST,
        function ($fd, $what) use (&$clients, $base, $serverSock) {
            while (true) {
                $conn = @socket_accept($serverSock);   // <-- use the real socket
                if ($conn === false) {
                    $err = socket_last_error($serverSock);
                    if ($err === SOCKET_EAGAIN || $err === SOCKET_EWOULDBLOCK) {
                        break;
                    }
                    break;
                }

                socket_set_nonblock($conn);

                $ip = 'unknown';
                $p  = 0;
                @socket_getpeername($conn, $ip, $p);

                $cfd = random_int(10000, 99999);
                $clients[$cfd] = [
                    'sock' => $conn,
                    'peer' => $ip . ':' . $p,
                    'ev'   => null,
                ];

                $clients[$cfd]['ev'] = new Event(
                    $base,
                    $conn,
                    Event::READ | Event::PERSIST,
                    function ($cfd2, $what2) use (&$clients) {
                        $cfd2 = intval($cfd2);
                        if (!isset($clients[$cfd2])) return;

                        $sock = $clients[$cfd2]['sock'];

                        $data = @socket_read($sock, 65536, PHP_BINARY_READ);
                        if ($data === '' || $data === false) {
                            $clients[$cfd2]['ev']->del();
                            @socket_close($sock);
                            unset($clients[$cfd2]);
                            return;
                        }

                        // Echo back (minimal)
                        $len = strlen($data);
                        $off = 0;
                        while ($off < $len) {
                            $sent = @socket_write($sock, substr($data, $off));
                            if ($sent === false || $sent === 0) break;
                            $off += $sent;
                        }
                    }
                );

                $clients[$cfd]['ev']->add();
            }
        }
    );

    $acceptEvent->add();

    fwrite(STDOUT, "[worker {$pid}] listening on {$host}:{$port}\n");

    $base->loop();

    foreach ($clients as $c) {
        @socket_close($c['sock']);
    }
}