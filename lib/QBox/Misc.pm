#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Misc
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Misc;

use strict;
use warnings;

use MIME::Base64;

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_base64_encode
    qbox_base64_encode_urlsafe
    qbox_base64_decode
    qbox_base64_decode_urlsafe
    qbox_make_entry
);

sub qbox_base64_encode {
    my $str = shift;
    my $encoded_str = encode_base64($str);
    $encoded_str =~ s/\n//g;
    return $encoded_str;
} # qbox_base64_encode

sub qbox_base64_decode {
    my $str = shift;
    return decode_base64("$str\n");
} # qbox_base64_decode

sub qbox_base64_encode_urlsafe {
    my $str = shift;
    my $encoded_str = qbox_base64_encode($str);
    $encoded_str =~ y,+/,-_,;
    return $encoded_str;
} # qbox_base64_encode_urlsafe

sub qbox_base64_decode_urlsafe {
    my $str = shift;
    $str =~ y,-_,+/,;
    return qbox_base64_decode($str);
} # qbox_base64_decode_urlsafe

sub qbox_make_entry {
    my $bucket = shift;
    my $key    = shift;
    return "${bucket}:${key}";
} # qbox_make_entry

1;

__END__
