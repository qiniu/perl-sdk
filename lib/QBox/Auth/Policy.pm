#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Auth::Policy
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Auth::Policy;

use strict;
use warnings;

use JSON qw();   # external library

my %fields = (
    scope            => undef,
    deadline         => 3600,
    callbackUrl      => undef,
    callbackBodyType => undef,
    customer         => undef,
    escape           => 0,
);

our $AUTOLOAD;

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $args  = shift || {};
    my $self  = bless { fields => {} }, $class;

    foreach my $key (keys(%fields)) {
        $self->$key($args->{$key});
    } # foreach

    return $self;
} # new

sub deadline {
    my $self    = shift;
    my $new_val = shift;
    my $old_val = $self->{expires_in};

    if ($new_val) {
        $self->{fields}{deadline} = $new_val + time();
        $self->{expires_in} = $new_val;
    }

    return $old_val;
} # deadline

sub AUTOLOAD {
    my $self    = shift;
    my $new_val = shift;
    my $key     = $AUTOLOAD;
    $key =~ s/.*://;

    my $old_val = $self->{fields}{$key};

    if ($new_val) {
        $self->{fields}{$key} = $new_val;
    }

    return $old_val;
} # AUTOLOAD

sub to_json {
    my $self = shift;
    return JSON::to_json($self->{fields});
} # to_json

1;

__END__
