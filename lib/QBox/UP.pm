#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::UP
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::UP;

use strict;
use warnings;

use Digest::CRC qw(crc32); # external library

use QBox::Config;
use QBox::Client;
use QBox::Misc;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_up_new_progress

    qbox_up_init
    qbox_up_mkblock
    qbox_up_blockpu
    qbox_up_resumable_blockput
    qbox_up_mkfile
    qbox_up_put
);

my $encoded_mime_type = qbox_base64_encode_urlsafe('application/octet-stream');

### procedures
sub qbox_up_new_progress {
    return &new_progress;
} # qbox_up_new_progress

sub qbox_up_init {
    return &new;
} # qbox_up_init

sub qbox_up_mkblock {
    return &mkblock;
} # qbox_up_mkblock

sub qbox_up_blockput {
    return &blockput;
} # qbox_up_blockput

sub qbox_up_resumable_blockput {
    return &resumable_blockput;
} # qbox_up_resumable_blockput

sub qbox_up_mkfile {
    return &mkfile;
} # qbox_up_mkfile

sub qbox_up_put {
    return &put;
} # qbox_up_put

### package functions
sub reform_checksums {
    my $checksums  = shift;
    my $grep_valid = shift;

    my $new_checksums = ($grep_valid) ? [grep {$_->{value}} @{$checksums}] : $checksums;

    my $buff = join '', map { qbox_base64_decode_urlsafe($_->{value} || q{}) } @{$new_checksums};
    my $size = 20 * scalar(@{$new_checksums});
    return $buff, $size;
} # reform_checksums

### for OOP
my $qbox_up_chunk_put = sub {
    my $self     = shift;
    my $body     = shift;
    my $body_len = shift;
    my $url      = shift;

    my ($body_data, $byes) = $body->{read}->($body_len, $body->{uservar});
    $url .= '/mimeType/' . $encoded_mime_type;
    my ($ret, $err) = $self->{client}->call_with_buffer($url, $body_data, $body_len);

    if ($err->{code} != 200) {
        return undef, $err;
    }

    my $chunk_crc32 = crc32($body_data);
    if ($chunk_crc32 != $ret->{crc32}) {
        $ret            = undef;
        $err->{code}    = 400;
        $err->{message} = 'Failed in verifying chunk CRC32';
    }

    return $ret, $err;
};

sub new_progress {
    my $fsize = shift;

    my $rest = $fsize;
    my $prog = {};

    $prog->{blk_count} = int(($fsize + ((1 << 22) - 1)) / QBOX_UP_BLOCK_SIZE);
    $prog->{checksums} = [];
    $prog->{progs}     = [];
    
    for (my $i = 0; $i < $prog->{blk_count}; ++$i) {
        push @{$prog->{checksums}}, { value => undef };
        push @{$prog->{progs}}, {};

        $prog->{progs}[$i]{ctx}      = undef;
        $prog->{progs}[$i]{offset}   = 0;
        $prog->{progs}[$i]{err_code} = 0;
        $prog->{progs}[$i]{rest_size} = ($rest > QBOX_UP_BLOCK_SIZE) ? QBOX_UP_BLOCK_SIZE : $rest;

        $rest -= $prog->{progs}[$i]{rest_size};
    } # for

    return $prog;
} # new_progress

sub new {
    my $class = shift || __PACKAGE__;
    my $client = shift;
    my $hosts  = shift || {};

    $hosts->{up_host} ||= QBOX_UP_HOST;

    my $self = {
        client => $client,
        hosts  => $hosts,
    };
    return bless $self, $class;
} # new

sub mkblock {
    my $self     = shift;
    my $blk_size = shift;
    my $body     = shift;
    my $body_len = shift;

    my $url = "$self->{hosts}{up_host}/mkblk/${blk_size}";
    return $qbox_up_chunk_put->($self, $body, $body_len, $url);
} # mkblock

sub blockput {
    my $self     = shift;
    my $ctx      = shift;
    my $offset   = shift;
    my $body     = shift;
    my $body_len = shift;

    my $url = "$self->{hosts}{up_host}/bput/${ctx}/${offset}";
    return $qbox_up_chunk_put->($self, $body, $body_len, $url);
} # blockput

my $read_part = sub {
    my ($maxlen, $uservar) = @_;

    my $data = "";
    my $bytes = $uservar->{reader_at}->{read_at}(
        \$data,
        $uservar->{offset},
        $maxlen,
        $uservar->{reader_at}
    );

    return $data, $bytes;
};

my $qbox_up_try_put = sub {
    my $self          = shift;
    my $action        = shift;
    my $body          = shift;
    my $blk_index     = shift;
    my $blk_size      = shift;
    my $chk_size      = shift;
    my $retry_times   = shift;
    my $blk_prog      = shift;
    my $chk_notify    = shift;
    my $notify_params = shift;

    my $ret = undef;
    my $err = {};
    my $keep_going = 1;
    my $body_len = ($blk_prog->{rest_size} > $chk_size) ? $chk_size : $blk_prog->{rest_size};
    $body->{offset} = ($blk_index * $blk_size) + $blk_prog->{offset};

    for (my $i = 0; $i <= $retry_times; ++$i) {
        ($ret, $err) = $action->($body, $body_len);

        if ($err->{code} == 200) {
            $blk_prog->{ctx}        = $ret->{ctx};
            $blk_prog->{rest_size} -= $body_len;
            $blk_prog->{offset}    += $body_len;

            if ($chk_notify) {
                $keep_going = $chk_notify->($notify_params, $blk_index, $blk_prog);
            }

            last;
        }
    } # for

    if ($err->{code} != 200) {
        return undef, $err, 0;
    }
    if ($keep_going == 0) {
        $err->{code}    = 299;
        $err->{message} = 'The chunk has been put but the progress is aborted';
        return undef, $err, 0;
    }

    $err->{code}    = 200;
    $err->{message} = 'OK';
    return $ret, $err, $keep_going;
};

sub resumable_blockput {
    my $self      = shift;
    my $reader_at = shift;

    my ($blk_index, $blk_size, undef, undef, $blk_prog) = @_;

    my $body = { read => $read_part, reader_at => $reader_at };
    $body->{uservar} = $body;

    my $keep_going = 1;
    my $ret = undef;
    my $err = undef;

    if ($blk_prog->{rest_size} == $blk_size) {
        ($ret, $err, $keep_going) = $qbox_up_try_put->(
            $self,
            sub { return mkblock($self, $blk_size, @_); },
            $body,
            @_
        );
    } # make block

    if ($keep_going == 0) {
        return $ret, $err;
    }

    # Try put block
    while ($blk_prog->{rest_size} > 0) {
        ($ret, $err, $keep_going) = $qbox_up_try_put->(
            $self,
            sub { return blockput($self, $blk_prog->{ctx}, $blk_prog->{offset}, @_); },
            $body,
            @_
        );

        if ($keep_going == 0) {
            return $ret, $err;
        }
    } # while putting block

    $err->{code}    = 200;
    $err->{message} = 'OK';
    return $ret, $err;
} # resumable_blockput

sub mkfile {
    my $self            = shift;
    my $cmd             = shift;
    my $entry           = shift;
    my $mime_type       = shift || 'application/octet-stream';
    my $fsize           = shift;
    my $params          = shift;
    my $callback_params = shift;
    my $checksums       = shift;
    my $blk_count       = shift;

    my @args = (
        $self->{hosts}{up_host},
        $cmd    => qbox_base64_encode_urlsafe($entry),
        'fsize' => $fsize,
    );

    if ($params and $params ne q{}) {
        push @args, $params;
    }

    push @args, 'mimeType', qbox_base64_encode_urlsafe($mime_type);

    if ($callback_params and $callback_params ne q{}) {
        push @args, 'params', $callback_params;
    }

    my $url = join('/', @args);
    my ($cksum_buff, $cksum_size) = reform_checksums($checksums);
    return $self->{client}->call_with_buffer($url, $cksum_buff, $cksum_size);
} # mkfile

sub put {
    my $self          = shift;
    my $reader_at     = shift;
    my $fsize         = shift;
    my $prog          = shift;
    my $blk_notify    = shift;
    my $chk_notify    = shift;
    my $notify_params = shift;

    # Find next block
    my $blk_index = 0;
    for (my $i = 0; $i < $prog->{blk_count}; ++$i) {
        my $rest_size = $prog->{progs}[$i]{rest_size};
        if (defined($rest_size) and $rest_size > 0) {
            $blk_index = $i;
            last;
        }
    } # for

    my $keep_going = 1;
    my $ret = undef;
    my $err = {};
    for (; $blk_index < $prog->{blk_count}; ++$blk_index) {
        ($ret, $err) = $self->resumable_blockput(
            $reader_at,
            $blk_index,
            $prog->{progs}[$blk_index]{offset} + $prog->{progs}[$blk_index]{rest_size},
            QBOX_PUT_CHUNK_SIZE,
            QBOX_PUT_RETRY_TIMES,
            $prog->{progs}[$blk_index],
            $chk_notify,
            $notify_params
        );

        if ($err->{code} != 200) {
            return $ret, $err;
        }

        $prog->{checksums}[$blk_index]{value} = $ret->{checksum};

        if ($blk_notify) {
            $keep_going = $blk_notify->($notify_params, $blk_index, $prog->{checksums}[$blk_index]);
        }
        if ($keep_going == 0 && ($blk_index + 1) < $prog->{blk_count}) {
            $err->{code}    = 299;
            $err->{message} = 'The block has been put but the progress is aborted';
            return $ret, $err;
        }
    } # for

    $err->{code} = 200;
    $err->{message} = 'OK';
    return $ret, $err;
} # put

sub query {
    my $self      = shift;
    my $checksums = shift;

    my $url = "$self->{hosts}{up_host}/query";
    my ($cksum_buff, $cksum_size) = reform_checksums($checksums, q{grep valid});
    my ($ret, $err) = $self->{client}->call_with_buffer(
        $url,
        $cksum_buff,
        $cksum_size,
        { as_verbatim => 1 }
    );
    return $ret, $err;
} # query

1;

__END__
