#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Config
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Config;

use strict;
use warnings;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    QBOX_UP_HOST
    QBOX_IO_HOST
    QBOX_RS_HOST
    QBOX_EU_HOST

    QBOX_PUT_CHUNK_SIZE
    QBOX_PUT_RETRY_TIMES
    QBOX_UP_BLOCK_SIZE
);

use constant QBOX_UP_HOST => 'http://up.qbox.me';
use constant QBOX_IO_HOST => 'http://iovip.qbox.me';
use constant QBOX_RS_HOST => 'http://rs.qbox.me:10100';
use constant QBOX_EU_HOST => 'http://eu.qbox.me';

use constant QBOX_PUT_CHUNK_SIZE  => 256 * 1024;
use constant QBOX_PUT_RETRY_TIMES => 2;
use constant QBOX_UP_BLOCK_SIZE   => 4 * 1024 * 1024;

1;

__END__
