#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Auth::Digest
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Auth::Digest;

use strict;
use warnings;

use Digest::SHA qw(hmac_sha1);

use QBox::Misc;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_auth_digest_init
    qbox_auth_digest_gen_headers
);

### procedures
my $qbox_auth_digest_gen_sha1 = sub {
    my $scr_key  = shift;
    my $url      = shift;
    my $addition = shift || q{};

    if ($url !~ m,^\w+://[^/]+(.+),o) {
        return undef, 'Invalid URL';
    }

    my $path         = $1;
    my $data         = $path . "\n" . $addition;
    my $sha1         = hmac_sha1($data, $scr_key);
    my $encoded_sha1 = qbox_base64_encode_urlsafe($sha1);

    return $encoded_sha1;
};

sub qbox_auth_digest_init {
    return &new;
} # qbox_auth_digest_init

sub qbox_auth_digest_gen_headers {
    return &gen_headers;
} # qbox_auth_digest_gen_headers

### for OOP
sub new {
    my $class = shift || __PACKAGE__;
    my $access_key = shift;
    my $secret_key = shift;
    my $self = {
        acs_key => $access_key,
        scr_key => $secret_key,
    };
    return bless $self, $class;
} # new

sub gen_headers {
    my $self = shift;
    my $url  = shift;

    my $encoded_sha1 = $qbox_auth_digest_gen_sha1->($self->{scr_key}, $url);
    my $headers      = {
        "Authorization" => "QBox " . $self->{acs_key} . ":" . $encoded_sha1,
    };

    return $headers;
} # gen_headers

1;

__END__
