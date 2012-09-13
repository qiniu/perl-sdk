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

    qbox_hash_merge

    qbox_make_entry
    qbox_extract_args
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

sub qbox_hash_merge {
    my $to   = shift;
    my $from = shift;
    my $base = shift;
    my $keys = shift;
    
    if (defined($base) and uc("$base") eq 'FROM') {
        $keys ||= [keys(%$from)];
    }
    else {
        $keys ||= [keys(%$to)];
    }

    foreach my $key (@$keys) {
        if (ref($from->{$key}) eq 'HASH') {
            $to->{$key} ||= {};
            qbox_hash_merge($to->{$key}, $from->{$key}, $base);
        }
        else {
            $to->{$key} = $from->{$key};
        }
    } # foreach

    return $to;
} # qbox_hash_merge

sub qbox_make_entry {
    my $bucket = shift;
    my $key    = shift;
    return "${bucket}:${key}";
} # qbox_make_entry

sub qbox_extract_args {
    my $arg_list = shift;
    if (scalar(@_) == 2 and ref($_[0]) eq 'HASH' and ref($_[1]) eq 'HASH') {
        push @$arg_list, 'opts';
        $_[0]->{opts} = $_[1];
        return map { $_[0]->{$_} } @$arg_list;
    }
    return splice @_, 0, scalar(@$arg_list);
} # qbox_extract_args

1;

__END__
