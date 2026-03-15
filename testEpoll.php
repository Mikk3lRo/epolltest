<?php declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "Run from CLI.\n");
    exit(1);
}
if (!extension_loaded('sockets')) {
    fwrite(STDERR, "Missing ext-sockets\n");
    exit(1);
}
if (!extension_loaded('event')) {
    fwrite(STDERR, "Missing ext-event\n");
    exit(1);
}
if (!defined('SO_REUSEPORT')) {
    fwrite(STDERR, "Missing SO_REUSEPORT\n");
    exit(1);
}

require_once __DIR__ . '/abstractListener.php';
require_once __DIR__ . '/echoListener.php';

$host       = '0.0.0.0';
$port       = 49756;
$numWorkers = 5;

$listener = new echoListener($host, $port, $numWorkers);

$listener->runMaster();

