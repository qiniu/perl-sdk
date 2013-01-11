#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Auth::UpToken
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Auth::UpToken;

use strict;
use warnings;

use JSON;   # external library

use Digest::SHA qw(hmac_sha1);
use QBox::Auth::Digest;
use QBox::Misc;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_auth_make_uptoken

    qbox_auth_uptoken_init
    qbox_auth_uptoken_gen_headers
);

### procedures
sub qbox_auth_make_uptoken {
    my $acs_key = shift;
    my $scr_key = shift;
    my $policy  = shift;

    my $policy_type = ref($policy);
    my $policy_str  = $policy || "";
    
    if ($policy_type eq 'HASH') {
        $policy_str = to_json($policy);
    }
    elsif ($policy_type eq 'QBox::Auth::Policy') {
        $policy_str = $policy->to_json();
    }

    my $encoded_policy = qbox_base64_encode_urlsafe($policy_str);
    my $sha1           = hmac_sha1($encoded_policy, $scr_key);
    my $encoded_sha1   = qbox_base64_encode_urlsafe($sha1);

    my $uptoken        = $acs_key . ":" . $encoded_sha1 . ":" . $encoded_policy;

    return $uptoken;
} # qbox_auth_make_uptoken

sub qbox_auth_uptoken_init {
    return &new;
} # qbox_auth_uptoken_init

sub qbox_auth_uptoken_gen_headers {
    return &gen_headers;
} # qbox_auth_uptoken_gen_headers

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $access_key = shift;
    my $secret_key = shift;
    my $policy     = shift;

    my $self = {
        acs_key => $access_key,
        scr_key => $secret_key,
        policy  => $policy,
    };

    bless $self, $class;
    $self->gen_uptoken($self->{policy});

    return $self;
} # new

sub gen_uptoken {
    my $self   = shift;
    my $policy = shift;

    if (defined($policy)) {
        $self->{uptoken} = qbox_auth_make_uptoken($self->{acs_key}, $self->{scr_key}, $policy);
    }
    return $self->{uptoken};
} # get_uptoken

sub gen_headers {
    my $self = shift;

    my $uptoken = $self->gen_uptoken(@_);
    my $headers = {
        "Authorization" => "UpToken $uptoken",
    };

    return $headers;
} # gen_headers

1;

__END__
