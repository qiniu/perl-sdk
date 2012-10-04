#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::EU
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::EU;

use strict;
use warnings;

use QBox::Config;
use QBox::Misc;

use constant API_WMSET       => 'eu.wmset';
use constant API_WMGET       => 'eu.wmget';
use constant API_ADMIN_WMGET => 'eu.admin-wmget';

### for OOP
my $qbox_eu_wmget = sub {
    my $self     = shift;
    my $api      = shift;
    my $customer = shift;
    my $query    = shift || {};
    my $opts     = shift || {}; 

    $opts->{_api} ||= $api;

    if (defined($customer) and "$customer" ne q{}) {
        $query->{customer} = "$customer";
    }

    my $url = "$self->{hosts}{eu_host}/${api}";
    my $ret = undef;
    my $err = undef;

    if (scalar(keys(%$query)) > 0) {
        ($ret, $err) = $self->{client}->call_with_multipart_form(
            $url,
            $query,
            undef,   # no body length
            $opts
        );
    }
    else {
        ($ret, $err) = $self->{client}->call($url, $opts);
    }

    return $ret, $err;
};

sub new {
    my $class  = shift || __PACKAGE__;
    my $client = shift;
    my $hosts  = shift || {};

    $hosts->{eu_host} ||= QBox::Config::QBOX_EU_HOST;

    my $self = {
        client => $client,
        hosts  => $hosts,
    };

    return bless $self, $class;
} # new

my $wm_settings = {
    customer  => undef,
    font      => undef,
    fontsize  => 0,             # 0 means using default value, unit: 1/20 pt
    fill      => undef,
    text      => undef,
    bucket    => undef,
    dissolve  => undef,
    gravity   => q{SouthEast},
    dx        => 10,
    dy        => 10,
};

sub wm_setting_names {
    return [keys(%$wm_settings)];
} # wm_setting_names 

sub wmset {
    my $self = shift;
    my ($settings, $opts) = qbox_extract_args([qw{settings}], @_);

    my $new_settings = {};
    foreach my $key (keys(%$settings)) {
        next unless (exists($wm_settings->{$key}));
        if (defined($settings->{$key})) {
            $new_settings->{$key} = $settings->{$key};
        }
        elsif (defined($wm_settings->{$key})) {
            $new_settings->{$key} = $wm_settings->{$key};
        }
    } # foreach

    $opts ||= {};
    $opts->{_api} = API_WMSET;
    my $url = "$self->{hosts}{eu_host}/wmset";
    return $self->{client}->call_with_multipart_form(
        $url,
        $new_settings,
        undef,           # no body length
        $opts
    );
} # wmset

sub wmget {
    my $self = shift;
    my ($customer, $query, $opts) = qbox_extract_args([qw{customer query}], @_);
    $opts ||= {};
    $opts->{_api} = API_WMGET;
    return $self->$qbox_eu_wmget('wmget', $customer, $query, $opts);
} # wmget

sub admin_wmget {
    my $self = shift;
    my ($id, $customer, $opts) = qbox_extract_args([qw{id customer}], @_);

    if (not defined($id) or $id eq q{}) {
        return undef, { code => 499, message => 'Invalid User ID' };
    }

    $opts ||= {};
    $opts->{_api} = API_ADMIN_WMGET;
    my $query = { id => $id };
    return $self->$qbox_eu_wmget('admin/wmget', $customer, $query, $opts);
} # admin_wmget

1;

__END__
