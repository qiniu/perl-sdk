#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Auth::Password
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Auth::Password;

use strict;
use warnings;

use JSON;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_auth_password_init
    qbox_auth_password_gen_headers
);

### procedures
sub qbox_auth_password_init {
    return &new;
} # qbox_auth_password_init

sub qbox_auth_password_gen_headers {
    return &gen_headers;
} # qbox_auth_password_gen_headers

### for OOP
sub new {
    my $class    = shift || __PACKAGE__;
    my $token    = shift;
    my $username = shift;
    my $password = shift;

    my $self = {
        token    => $token,
        username => $username,
        password => $password,
    };
    return bless $self, $class;
} # new

sub gen_headers {
    my $self = shift;
    my $url  = shift;

    if (not exists($self->{expiry})) {
        my $err = $self->{token}->exchange_by_password($self->{username}, $self->{password});
        return [] if ($err->{code} != 200);
    }
    else {
        if ($self->{expiry} <= time()) {
            my $err = $self->{token}->exchange_by_refresh_token();
            return [] if ($err->{code} != 200);
        }    
    }

    my ($acs_token, $expiry) = $self->{token}->access();
    $self->{expiry} = $expiry;

    my $headers = {
        "Authorization" => "Bearer " . $acs_token,
    };
    return $headers;
} # gen_headers

1;

__END__
