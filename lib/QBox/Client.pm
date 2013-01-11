#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Client
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Client;

use strict;
use warnings;
use English;

use JSON;                            # external library
use Net::Curl::Easy qw(:constants);  # external library
use Net::Curl::Form qw(:constants);  # external library

use QBox::Base::Curl;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_client_init
    qbox_client_call
    qbox_client_call_with_binary
    qbox_client_call_with_buffer
    qbox_client_call_with_form
    qbox_client_call_with_multipart_form
);

### procedures
my $qbox_client_gen_headers = sub {
    my $self    = shift;
    my $url     = shift;

    my $headers = undef;
    my $auth    = $self->{auth};

    if (defined($auth)) {
        my $auth_type = ref($auth);

        if ($auth_type eq 'HASH' and defined($auth->{gen_headers})) {
            $headers = $auth->{gen_headers}($auth, $url);
        }
        else {
            $headers = $auth->gen_headers($url);
        }
    }

    return $headers;
};

sub qbox_client_init {
    return &new;
} # qbox_client_init

sub qbox_client_call {
    return &call;
} # qbox_client_call

sub qbox_client_call_with_binary {
    return &call_with_binary;
} # qbox_client_call_with_binary

sub qbox_client_call_with_buffer {
    return &call_with_buffer;
} # qbox_client_call_with_buffer

sub qbox_client_call_with_form {
    return &call_with_form;
} # qbox_client_call_with_form

sub qbox_client_call_with_multipart_form {
    return &call_with_multipart_form;
} # qbox_client_call_with_multipart_form

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $auth = shift;
    my $self = { auth => $auth };
    return bless $self, $class;
} # new

sub call {
    my $self         = shift;
    my $url          = shift;
    my $opts         = shift;

    my $headers = $qbox_client_gen_headers->($self, $url);
    $headers = qbox_curl_merge_headers($headers, $opts->{_headers});

    my $curl = qbox_curl_call_pre($url, $headers, $opts);
    return qbox_curl_call_core($curl, $opts);
} # call

sub call_with_binary {
    my $self         = shift;
    my $url          = shift;
    my $body         = shift;
    my $body_len     = shift;
    my $opts         = shift;

    my $headers = $qbox_client_gen_headers->($self, $url);
    $headers = qbox_curl_merge_headers(
        $headers,
        {
            "Content-Type"   => "application/octet-stream",
            "Content-Length" => "${body_len}",
        },
        $opts->{_headers}
    );

    my $curl = qbox_curl_call_pre($url, $headers, $opts);

    $curl->setopt(CURLOPT_POST,         1);
    $curl->setopt(CURLOPT_INFILESIZE,   $body_len);
    $curl->setopt(CURLOPT_READFUNCTION, $body->{read});
    $curl->setopt(CURLOPT_READDATA,     $body->{uservar});

    return qbox_curl_call_core($curl, $opts);
} # call_with_binary

sub call_with_buffer {
    my $self         = shift;
    my $url          = shift;
    my $body         = shift;
    my $body_len     = shift;
    my $opts         = shift;

    if (ref($body) ne q{}) {
        return undef, { code => 499, message => 'Invalid buffer body' };
    }

    my $headers = $qbox_client_gen_headers->($self, $url);
    $headers = qbox_curl_merge_headers(
        $headers,
        {
            "Content-Type"   => "application/octet-stream",
            "Content-Length" => "${body_len}",
        },
        $opts->{_headers}
    );

    my $curl = qbox_curl_call_pre($url, $headers, $opts);

    $curl->setopt(CURLOPT_POST,          1);
    $curl->setopt(CURLOPT_POSTFIELDSIZE, $body_len);
    $curl->setopt(CURLOPT_POSTFIELDS,    $body);

    return qbox_curl_call_core($curl, $opts);
} # call_with_buffer

sub call_with_form {
    my $self         = shift;
    my $url          = shift;
    my $body         = shift;
    my $body_len     = shift;
    my $opts         = shift;

    if (ref($body) ne q{HASH}) {
        return undef, { code => 499, message => 'Invalid form body' };
    }

    my $headers = $qbox_client_gen_headers->($self, $url);
    $headers = qbox_curl_merge_headers($headers, $opts->{_headers});

    my $curl = qbox_curl_call_pre($url, $headers, $opts);

    my $form = qbox_curl_make_form($body);
    $curl->setopt(CURLOPT_POSTFIELDS, $form);

    return qbox_curl_call_core($curl, $opts);
} # call_with_form

sub call_with_multipart_form {
    my $self         = shift;
    my $url          = shift;
    my $body         = shift;
    my $body_len     = shift;
    my $opts         = shift;

    my $headers = $qbox_client_gen_headers->($self, $url);
    $headers = qbox_curl_merge_headers($headers, $opts->{_headers});

    my $curl = qbox_curl_call_pre($url, $headers, $opts);

    my $form = undef;
    if (ref($body) eq 'HASH') {
        $form = qbox_curl_make_multipart_form($body);
    }
    elsif (ref($body) eq 'ARRAY') {
        $form = qbox_curl_make_multipart_form(@$body);
    }
    else {
        return undef, { code => 499, message => 'Invalid form body' };
    }

    $curl->setopt(CURLOPT_HTTPPOST, $form);

    return qbox_curl_call_core($curl, $opts);
} # call_with_multipart_form

1;

__END__
