#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::ReaderAt::File
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::ReaderAt::File;

use strict;
use warnings;

use English;
use Fcntl qw(SEEK_SET);

### procedures
my $qbox_read_at = sub {
    my ($data, $offset, $maxlen, $uservar) = @_;
    seek($uservar->{fh}, $offset, SEEK_SET);
    return sysread($uservar->{fh}, $$data, $maxlen);
};

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $file  = shift;

    my $fh = undef;

    die 'Need filename' unless defined($file) && $file ne q{};
    open($fh, '<', $file) or die "$OS_ERROR";

    my $self = {};
    $self->{fh}      = $fh;
    $self->{read_at} = $qbox_read_at;
    $self->{uservar} = $self;

    return bless $self, $class;
} # new

sub open {
    return &new;
} # open

sub read_at {
    my $self = shift;
    return &{$self->{read_at}};
} # read_at

sub close {
    my $self = shift;
    close($self->{fh}) if $self && $self->{fh};
} # close

1;

__END__
