<?php declare(strict_types = 1);

class echoListener extends abstractListener
{
    public function onData(string $data, $client)
    {
        // Echo the data back to the sender.
        $data = 'You said: ' . $data;

        $len = strlen($data);
        $off = 0;
        while ($off < $len) {
            $sent = @socket_write($client['sock'], substr($data, $off));
            if ($sent === false || $sent === 0) {
                break;
            }
            $off += $sent;
        }
    }
}
