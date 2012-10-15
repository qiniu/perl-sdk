#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Base::Curl
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Base::Curl;

use strict;
use warnings;
use English;

use JSON;                            # external library
use URI::Escape;                     # external library
use Net::Curl::Easy qw(:constants);  # external library
use Net::Curl::Form qw(:constants);  # external library

use QBox::Stub;
use QBox::Misc;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_curl_call_pre
    qbox_curl_call_core
    qbox_curl_make_form 
    qbox_curl_make_multipart_form 
    qbox_curl_merge_headers
);

my $qbox_curl_gen_headers = sub {
    my $headers = shift;
    
    my $curl_headers = [ 
        map {
            "$_: $headers->{$_}"
        } grep {
            defined($headers->{$_})
        } keys(%$headers)
    ];

    return $curl_headers;
};

sub qbox_curl_call_pre {
    my $url     = shift;
    my $headers = shift || {};
    my $opts    = shift || {};

    my $curl = Net::Curl::Easy->new();

    if (ref($opts->{_headers}) eq 'HASH') {
        qbox_hash_merge($headers, $opts->{_headers}, 'FROM');
    }

    my $api = $opts->{_api} || 'unknown_api';
    QBox::Stub::call_stub("${api}.url", \$url);
    QBox::Stub::call_stub("${api}.headers", \$headers);

    $headers = $qbox_curl_gen_headers->($headers);

    $curl->setopt(CURLOPT_CUSTOMREQUEST,  'POST');
    $curl->setopt(CURLOPT_SSL_VERIFYPEER, 0);
    $curl->setopt(CURLOPT_SSL_VERIFYHOST, 0);
    $curl->setopt(CURLOPT_URL,            $url);

    if (ref($headers) eq 'ARRAY' and scalar(@$headers) > 0) {
        $curl->setopt(CURLOPT_HTTPHEADER, $headers);
    }

    return $curl;
} # qbox_curl_call_pre

my $qbox_curl_write_data = sub {
    my ($curl, $data, $resp) = @_;
    $$resp .= $data;
    return length $data;
};

sub qbox_curl_call_core {
    my $curl = shift;
    my $opts = shift || {};

    my $resp = '';

    $curl->setopt(CURLOPT_WRITEFUNCTION, $qbox_curl_write_data);
    $curl->setopt(CURLOPT_WRITEDATA, \$resp);

    eval {
        $curl->perform();
    };

    my $curl_error = $EVAL_ERROR;
    my $http_code = 0;
    my $ret = undef;
    my $err = {};

    if ($opts->{_as_verbatim}) {
        $ret = $resp;
    }
    elsif (length($resp) > 0) {
        eval { $ret = from_json($resp); };

        if ($EVAL_ERROR) {
            $ret = { 'response' => $resp };
        }
    } # if

    if ($curl_error) {
        $err->{code} = $curl_error + 0;
    }
    else {
        $http_code   = $curl->getinfo(CURLINFO_RESPONSE_CODE);
        $err->{code} = $http_code;
    }

    if (200 <= $http_code && $http_code <= 299) {
        $err->{message} = 'OK';
    }
    else {
        $err->{message} = ($opts->{_simple_error}) ? $opts->{_simple_error} : 
                          ($ret && $ret->{error}) ? $ret->{error}         :
                                                    $curl->error()        ;
    }

    return $ret, $err;
} # qbox_curl_call_core 

sub qbox_curl_make_form {
    my $query = shift;
    return join '&', map { sprintf("%s=%s", $_, uri_escape($query->{$_})) } keys(%$query);
} # qbox_curl_make_form

sub qbox_curl_make_multipart_form {
    my $fields      = shift;
    my $file_fields = shift;

    my $form = Net::Curl::Form->new();

    if (ref($fields) eq 'HASH') {
        foreach my $key (keys(%$fields)) {
            $form->add(
                CURLFORM_COPYNAME()     => $key,
                CURLFORM_COPYCONTENTS() => $fields->{$key}
            );
        } # foreach
    } # if

    if (ref($file_fields) eq 'HASH') {
        foreach my $key (keys(%$file_fields)) {
            $form->add(
                CURLFORM_COPYNAME() => $key,
                CURLFORM_FILE()     => $file_fields->{$key}
            );
        } # foreach
    } # if

    return $form;
} # qbox_curl_make_multipart_form

sub qbox_curl_merge_headers {
    my %headers = ();
    foreach my $headers (@_) {
        if (ref($headers) eq 'HASH') {
            qbox_hash_merge(\%headers, $headers, 'FROM');
        }
    } # foreach
    return \%headers;
} # qbox_curl_merge_headers

1;

__END__
