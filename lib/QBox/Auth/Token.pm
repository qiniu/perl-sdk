#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Auth::Token
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Auth::Token;

use strict;
use warnings;

use Net::Curl::Easy qw(:constants);  # external library

use QBox::Config;
use QBox::Base::Curl;

our @ISA = qw(Exporter);
our @EXPORT = qw(
);

### procedures
my $exchange = sub {
    my $self  = shift;
    my $query = shift;

    my $url = "$self->{hosts}{ac_host}/oauth2/token";
    my $curl = qbox_curl_call_pre($url);

    my $form = qbox_curl_make_form($query);
    $curl->setopt(CURLOPT_POSTFIELDS, $form);

    my ($ret, $err) = qbox_curl_call_core($curl);

    if ($err->{code} != 200) {
        return $err;
    }
    if (not defined($ret->{access_token}) or not defined($ret->{refresh_token})) {
        return {
            code    => 9998,
            message => "unexcepted response",
        };
    }

    $self->{token} = {
        access_token  => $ret->{access_token},
        refresh_token => $ret->{refresh_token},
        expiry        => time() + $ret->{expires_in},
    };

    return $err;
};

### for OOP
sub new {
    my $class         = shift || __PACKAGE__;
    my $hosts         = shift || {};

    $hosts->{ac_host} ||= QBox::Config::QBOX_AC_HOST;

    my $client_id     = shift;
    my $client_secret = shift;

    my $self = {
        hosts => $hosts,
        info => {
            client_id     => $client_id,
            client_secret => $client_secret,
        },
        token => {},
    };

    return bless $self, $class;
} # new

sub exchange_by_password {
    my $self     = shift;
    my $username = shift;
    my $password = shift;

    my $query = {
        client_id     => $self->{info}{client_id},
        client_secret => $self->{info}{client_secret},
        username      => $username,
        password      => $password,
        grant_type    => 'password',
    };

    return $exchange->($self, $query);
} # exchange_by_password

sub exchange_by_refresh_token {
    my $self     = shift;

    my $query = {
        client_id     => $self->{info}{client_id},
        client_secret => $self->{info}{client_secret},
        refresh_token => $self->{token}{refresh_token},
        grant_type    => 'refresh_token',
    };

    return $exchange->($self, $query);
} # exchange_by_refresh_token

sub access {
    my $self = shift;
    return $self->{token}{access_token}, $self->{token}{expiry};
} # access

1;

__END__
