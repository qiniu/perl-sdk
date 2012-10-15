#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::UC
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::UC;

use strict;
use warnings;

use QBox::Config;
use QBox::Misc;

use constant API_APP_INFO      => 'uc.app-info';
use constant API_NEW_ACCESS    => 'uc.new-access';
use constant API_DELETE_ACCESS => 'uc.delete-access';

sub new {
    my $class = shift || __PACKAGE__;
    my $client = shift;
    my $hosts  = shift || {};

    $hosts->{uc_host} ||= QBox::Config::QBOX_UC_HOST;

    my $self = {
        client => $client,
        hosts  => $hosts,
    };
    return bless $self, $class;
} # new

sub app_info {
    my $self = shift;
    my ($app, $opts) = qbox_extract_args([qw{app}], @_);

    $opts ||= {};
    $opts->{_api} = API_APP_INFO;
    my $query = { app => $app };
    my $url = "$self->{hosts}{uc_host}/appInfo";
    return $self->{client}->call_with_form(
        $url,
        $query,
        undef,  # no body length
        $opts
    );
} # app_info

sub new_access {
    my $self = shift;
    my ($app, $opts) = qbox_extract_args([qw{app}], @_);

    $opts ||= {};
    $opts->{_api} = API_NEW_ACCESS;
    my $query = { app => $app };
    my $url = "$self->{hosts}{uc_host}/newAccess";
    return $self->{client}->call_with_form(
        $url,
        $query,
        undef,
        $opts
    );
} # new_access

sub delete_access {
    my $self = shift;
    my ($app, $acs_key, $opts) = qbox_extract_args([qw{app access_key}], @_);

    $opts ||= {};
    $opts->{_api} = API_DELETE_ACCESS;
    my $query = { app => $app, key => $acs_key };
    my $url = "$self->{hosts}{uc_host}/deleteAccess";
    return $self->{client}->call_with_form(
        $url,
        $query,
        undef,
        $opts
    );
} # delete_access

1;

__END__
