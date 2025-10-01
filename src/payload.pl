#!/usr/bin/env perl

use Socket;
my ($ip, $port) = @ARGV;
socket(my $sock, PF_INET, SOCK_STREAM, getprotobyname('tcp')) or die;
my $addr = inet_aton($ip) or die;
my $paddr = sockaddr_in($port, $addr);
connect($sock, $paddr) or die;
my $pid = fork;
die unless $pid;
my $buf;
if ($pid == 0) {
    for (;;) {
        my $bytes = sysread($socket, $buf, 4096);
        last unless $bytes;
        syswrite(STDOUT, $buf, $bytes);
    }
    exit;
} else {
    for (;;) {
        my $bytes = sysread(STDIN, $buf, 4096);
        last unless $bytes;
        syswrite($socket, $buf, $bytes);
    }
    waitpid($pid, 0);
}
