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

use constant API_MKBLOCK  => 'up.mkblock';
use constant API_BLOCKPUT => 'up.blockput';
use constant API_MKFILE   => 'up.mkfile';
use constant API_QUERY    => 'up.query';

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

sub qbox_up_put_blocks_one_by_one {
    return &put_blocks_one_by_one;
} # qbox_up_put_blocks_one_by_one

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
    my $opts     = shift;

    my ($body_data, $byes) = $body->{read}->($body_len, $body->{uservar});
    $url .= '/mimeType/' . $encoded_mime_type;
    my ($ret, $err) = $self->{client}->call_with_buffer($url, $body_data, $body_len, $opts);

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

        $prog->{progs}[$i]{ctx}       = undef;
        $prog->{progs}[$i]{offset}    = 0;
        $prog->{progs}[$i]{err_code}  = 0;
        $prog->{progs}[$i]{rest_size} = ($rest > QBOX_UP_BLOCK_SIZE) ? QBOX_UP_BLOCK_SIZE : $rest;

        $rest -= $prog->{progs}[$i]{rest_size};
    } # for

    return $prog;
} # new_progress

sub new {
    my $class = shift || __PACKAGE__;
    my $client = shift;
    my $hosts  = shift || {};

    $hosts->{up_host} ||= QBox::Config::QBOX_UP_HOST;

    my $self = {
        client => $client,
        hosts  => $hosts,
    };
    return bless $self, $class;
} # new

sub mkblock {
    my $self = shift;
    my ($blk_size, $body, $body_len, $opts) =
        qbox_extract_args([qw{blk_size body body_len}], @_);

    $opts ||= {};
    $opts->{api} = API_MKBLOCK;
    my $url = "$self->{hosts}{up_host}/mkblk/${blk_size}";
    return $qbox_up_chunk_put->($self, $body, $body_len, $url, $opts);
} # mkblock

sub blockput {
    my $self = shift;
    my ($ctx, $offset, $body, $body_len, $opts) =
        qbox_extract_args([qw{ctx offset body body_len}], @_);

    $opts ||= {};
    $opts->{api} = API_BLOCKPUT;
    my $url = "$self->{hosts}{up_host}/bput/${ctx}/${offset}";
    return $qbox_up_chunk_put->($self, $body, $body_len, $url, $opts);
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
    my $self = shift;
    my ($reader_at, $blk_index, $blk_size, undef, undef, $blk_prog) =
        qbox_extract_args([qw{reader_at blk_index blk_size chk_size retry_times blk_prog}], @_);
    shift @_;

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

sub mkfile_by_sha1 {
    my $self = shift;
    my ($cmd, $entry, $mime_type, $fsize, $params, $callback_params, $checksums, $blk_count, $opts) =
        qbox_extract_args([qw{cmd entry mime_type fsize params callback_params checksums blk_count}], @_);

    my @args = (
        $self->{hosts}{up_host},
        $cmd    => qbox_base64_encode_urlsafe("$entry"),
        'fsize' => "$fsize",
    );

    if (defined($params) and "$params" ne q{}) {
        push @args, "$params";
    }

    $mime_type = defined($mime_type) ? "$mime_type" : q{application/octet-stream};
    push @args, 'mimeType', qbox_base64_encode_urlsafe($mime_type);

    if (defined($callback_params) and "$callback_params" ne q{}) {
        push @args, 'params', qbox_base64_encode_urlsafe("$callback_params");
    }

    $opts ||= {};
    $opts->{api} = API_MKFILE;
    my $url = join('/', @args);
    my ($cksum_buff, $cksum_size) = reform_checksums($checksums);
    return $self->{client}->call_with_buffer($url, $cksum_buff, $cksum_size, $opts);
} # mkfile_by_sha1

sub mkfile {
    my $self = shift;
    my ($cmd, $entry, $mime_type, $fsize, $params, $callback_params, $prog, $opts) =
        qbox_extract_args([qw{cmd entry mime_type fsize params callback_params prog}], @_);

    my @args = (
        $self->{hosts}{up_host},
        $cmd    => qbox_base64_encode_urlsafe("$entry"),
        'fsize' => "$fsize",
    );

    if (defined($params) and "$params" ne q{}) {
        push @args, "$params";
    }

    $mime_type = defined($mime_type) ? "$mime_type" : q{application/octet-stream};
    push @args, 'mimeType', qbox_base64_encode_urlsafe($mime_type);

    if (defined($callback_params) and "$callback_params" ne q{}) {
        push @args, 'params', qbox_base64_encode_urlsafe("$callback_params");
    }

    $opts ||= {};
    $opts->{api} = API_MKFILE;
    $opts->{headers}{'Content-Type'} = 'text/plain';
    my $url = join('/', @args);
    my $ctx_buff = join ",", map { $_->{ctx} } @{$prog->{progs}};
    my $ctx_size = length($ctx_buff);
    return $self->{client}->call_with_buffer($url, $ctx_buff, $ctx_size, $opts);
} # mkfile

sub put_blocks_one_by_one {
    my $self = shift;
    my ($reader_at, $fsize, $prog, $blk_notify, $chk_notify, $notify_params, $opts) =
        qbox_extract_args([qw{reader_at fsize prog blk_notify chk_notify notify_params}], @_);

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
    my $err = undef;
    for (; $blk_index < $prog->{blk_count}; ++$blk_index) {
        ($ret, $err) = $self->resumable_blockput(
            $reader_at,
            $blk_index,
            $prog->{progs}[$blk_index]{offset} + $prog->{progs}[$blk_index]{rest_size},
            QBOX_PUT_CHUNK_SIZE,
            QBOX_PUT_RETRY_TIMES,
            $prog->{progs}[$blk_index],
            $chk_notify,
            $notify_params,
            $opts
        );
        return $ret, $err if ($err->{code} != 200);

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

    return $ret, { code => 200, message => 'OK' };
} # put_blocks_one_by_one

sub query {
    my $self = shift;
    my ($checksums, $opts) = qbox_extract_args([qw{checksums}], @_);

    $opts ||= {};
    $opts->{api}         = API_QUERY;
    $opts->{as_verbatim} = 1;
    my $url = "$self->{hosts}{up_host}/query";
    my ($cksum_buff, $cksum_size) = reform_checksums($checksums, q{grep valid});
    return $self->{client}->call_with_buffer($url, $cksum_buff, $cksum_size, $opts);
} # query

1;

__END__
