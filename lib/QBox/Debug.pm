#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Debug
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Debug;

use strict;
use warnings;

my $qbox_handler = undef;
my $qbox_data    = undef;

### for OOP
sub set_callback {
    my $handler = shift;
    my $data    = shift;

    if (ref($handler) eq q{CODE}) {
        $qbox_handler = $handler;
        $qbox_data    = $data;
    }
} # set_callback

sub callback {
    if ($qbox_handler) {
        $qbox_handler->($qbox_data, @_);
    }
} # callback

1;

__END__
