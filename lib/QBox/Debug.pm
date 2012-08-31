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

use constant TAG_ALL => 'all';

my %callback_settings = ();

### for OOP
sub set_callback {
    my $handler = shift;
    my $data    = shift;
    my $tag     = shift;
    
    $tag = defined($tag) ? "$tag" : TAG_ALL;

    if (ref($handler) eq q{CODE}) {
        $callback_settings{$tag} = {
            handler => $handler,
            data    => $data,
        };
        return 1;
    }

    return undef;
} # set_callback

sub unset_callback {
    my $tag = shift || TAG_ALL;

    $tag = defined($tag) ? "$tag" : TAG_ALL;

    if (exists($callback_settings{$tag})) {
        delete($callback_settings{$tag});
    }
} # unset_callback

sub callback {
    my ($tag) = @_;

    my $setting = $callback_settings{$tag};
    if (defined($setting)) {
        my $ret = $setting->{handler}->($setting->{data}, @_);
        return if not $ret; # no propogation
    }

    $setting =  $callback_settings{+TAG_ALL};
    if (defined($setting)) {
        $setting->{handler}->($setting->{data}, @_);
    }
} # callback

1;

__END__
