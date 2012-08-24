#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Reader::File
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Reader::File;

use strict;
use warnings;

use English;

### procedures
my $qbox_read_part = sub {
    my ($curl, $maxlen, $uservar) = @_;
    my $data = undef; 

    my $bytes = sysread($uservar->{fh}, $data, $maxlen);
    if ($bytes == 0) {
        close($uservar->{fh});
        $uservar->{fh} = undef;
    }
    
    return \$data;
};

sub qbox_reader_init {
    return &new;
} # qbox_reader_init

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $args  = shift || {};
    my $self = {};

    if ($args->{file} and $args->{file} ne q{}) {
        my $file  = $args->{file};

        my $fh = undef;

        open($fh, '<', $file) or die "$OS_ERROR";

        $self->{fh}   = $fh;
        $self->{read} = $qbox_read_part;
        $self->{to_be_closed} = 1;
    }
    elsif ($args->{fh} and $args->{read}) {
        $self->{fh}   = $args->{fh};
        $self->{read} = $args->{read};
    }

    $self->{uservar} = $self;

    return bless $self, $class;
} # new

sub open {
    return &new;
} # open

sub read {
    my $self = shift;
    if ($self->{to_be_closed}) {
        unshift @_, undef;
    }
    return $self->{read}->(@_);
} # read

sub close {
    my $self = shift;
    close($self->{fh}) if $self && $self->{fh} && $self->{to_be_closed};
} # close

1;

__END__
