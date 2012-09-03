#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Stub
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Stub;

use strict;
use warnings;

use constant API_ALL => 'all';

my %stub_settings = ();

### for OOP
sub set_stub {
    my $handler = shift;
    my $data    = shift;
    my $tag     = shift;
    
    $tag = defined($tag) ? "$tag" : API_ALL;

    if (ref($handler) eq q{CODE}) {
        $stub_settings{$tag} = {
            handler => $handler,
            data    => $data,
        };
        return 1;
    }

    return undef;
} # set_stub

sub unset_stub {
    my $tag = shift || API_ALL;

    $tag = defined($tag) ? "$tag" : API_ALL;

    if (exists($stub_settings{$tag})) {
        delete($stub_settings{$tag});
    }
} # unset_stub

sub call_stub {
    my ($tag) = @_;

    my $setting = $stub_settings{$tag};
    if (defined($setting)) {
        my $ret = $setting->{handler}->($setting->{data}, @_);
        return if not $ret; # no propogation
    }

    $setting =  $stub_settings{+API_ALL};
    if (defined($setting)) {
        $setting->{handler}->($setting->{data}, @_);
    }
} # call_stub

1;

__END__
