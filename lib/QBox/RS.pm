#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::RS
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::RS;

use strict;
use warnings;

use English;
use MIME::Base64;

use QBox::Config;
use QBox::Client;
use QBox::Reader::File;
use QBox::UP;
use QBox::Misc;

use constant API_GET         => 'rs.get';
use constant API_PUT         => 'rs.put';
use constant API_PUT_AUTH_EX => 'rs.put_auth_ex';
use constant API_STAT        => 'rs.stat';
use constant API_PUBLISH     => 'rs.publish';
use constant API_UNPUBLISH   => 'rs.unpublish';
use constant API_DELETE      => 'rs.delete';
use constant API_DROP        => 'rs.drop';

our @ISA = qw(Exporter);
our @EXPORT = qw(
    qbox_rs_init

    qbox_rs_get
    qbox_rs_get_if_not_modified

    qbox_rs_put
    qbox_rs_put_file
    qbox_rs_resumable_put
    qbox_rs_put_auth
    qbox_rs_put_auth_ex

    qbox_rs_stat
    qbox_rs_delete
    qbox_rs_drop
);

### procedures
sub qbox_rs_init {
    return &new;
} # qbox_rs_init

sub qbox_rs_get {
    return &get;
} # qbox_rs_get

sub qbox_rs_get_if_not_modified {
    return &get_if_not_modified;
} # qbox_rs_get_if_not_modified

sub qbox_rs_put {
    return &put;
} # qbox_rs_put

sub qbox_rs_put_file {
    return &put_file;
} # qbox_rs_put_file

sub qbox_rs_put_auth {
    return &put_auth;
} # qbox_rs_put_auth

sub qbox_rs_put_auth_ex {
    return &put_auth_ex;
} # qbox_rs_put_auth_ex

sub qbox_rs_resumable_put {
    return &resumale_put;
} # qbox_rs_resumable_put

sub qbox_rs_stat {
    my $self = shift;
    return &{$self->stat};
} # qbox_rs_stat

sub qbox_rs_delete {
    my $self = shift;
    return &{$self->delete};
} # qbox_rs_delete

sub qbox_rs_drop {
    return &drop;
} # qbox_rs_drop

### for OOP
sub new {
    my $class  = shift || __PACKAGE__;
    my $client = shift;
    my $hosts  = shift || {};

    $hosts->{rs_host} ||= QBox::Config::QBOX_RS_HOST;
    $hosts->{up_host} ||= QBox::Config::QBOX_UP_HOST;
    $hosts->{io_host} ||= QBox::Config::QBOX_IO_HOST;

    my $self = {
        client => $client,
        hosts  => $hosts,
    };
    return bless $self, $class;
} # new

sub get {
    my $self      = shift;
    my $bucket    = shift;
    my $key       = shift;
    my $attr_name = shift;
    my $base      = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key));
    my @args = (
        $self->{hosts}{rs_host},
        'get' => $encoded_entry,
    );

    $base      = defined($base)      ? "$base"      : q{};
    $attr_name = defined($attr_name) ? "$attr_name" : q{};

    if ($base ne q{}) {
        push @args, 'base', $base;
    }

    if ($attr_name ne q{}) {
        $attr_name = qbox_base64_encode_urlsafe($attr_name);
        push @args, 'attName', $attr_name;
    }

    my $url = join('/', @args);
    return $self->{client}->call($url, { 'api' => API_GET });
} # get

sub get_if_not_modified {
    return &get;
} # get_if_not_modified

sub put {
    my $self        = shift;
    my $bucket      = shift;
    my $key         = shift;
    my $mime_type   = shift;
    my $reader      = shift;
    my $fsize       = shift;
    my $custom_meta = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket      = "$bucket";
    $key         = "$key";
    $mime_type   = defined($mime_type)   ? "$mime_type"   : q{application/octet-stream};
    $custom_meta = defined($custom_meta) ? "$custom_meta" : q{};

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key));
    $mime_type = qbox_base64_encode_urlsafe($mime_type);

    my @args = (
        $self->{hosts}{up_host},
        'rs-put' => $encoded_entry,
        'mime'   => $mime_type,
    );

    if ($custom_meta ne q{}) {
        $custom_meta = qbox_base64_encode_urlsafe($custom_meta);
        push @args, 'meta', $custom_meta;
    }

    my $url = join('/', @args);
    return $self->{client}->call_with_binary(
        $url,
        $reader,
        $fsize,
        { 'api' => API_PUT }
    );
} # put

sub put_file {
    my $self        = shift;
    my $bucket      = shift;
    my $key         = shift;
    my $mime_type   = shift;
    my $file        = shift;
    my $custom_meta = shift;

    return undef, { code => 499, message => 'Invalid file' } if (not defined($file));

    $file = "$file";
    return undef, { code => 499, message => 'Cannot read file' } if (not -r $file);

    my $fsize  = (stat($file))[7];
    my $reader = QBox::Reader::File->new({ file => $file });

    # forward invocation
    return $self->put($bucket, $key, $mime_type, $reader, $fsize, $custom_meta);
} # put_file

# may be deprecated
sub put_auth {
    my $self       = shift;
    my $expires_in = shift || 10;
    return $self->put_auth_ex($expires_in);
} # put_auth

# may be deprecated
sub put_auth_ex {
    my $self       = shift;
    my $expires_in = shift;
    my $callback   = shift;

    return undef, { code => 499, message => 'Invalid expiry' } if (not defined($expires_in));

    my @args = (
        $self->{hosts}{io_host},
        'put-auth' => "$expires_in",
    );

    if ($callback) {
        $callback = qbox_base64_encode_urlsafe($callback);
        push @args, 'callback', $callback;
    }

    my $url = join('/', @args);
    return $self->{client}->call($url, { 'api' => API_PUT_AUTH_EX });
} # put_auth_ex

sub resumale_put {
    my $self            = shift;
    my $prog            = shift;
    my $blk_notify      = shift;
    my $chk_notify      = shift;
    my $notify_params   = shift;
    my $entry           = shift;
    my $mime_type       = shift;
    my $reader_at       = shift;
    my $fsize           = shift;
    my $custom_meta     = shift;
    my $other_params    = shift;
    my $callback_params = shift;

    return undef, { code => 499, message => 'Invalid entry' } if (not defined($entry));
    return undef, { code => 499, message => 'Invalid file size' } if (not defined($fsize));

    $entry           = "$entry";
    $mime_type       = defined($mime_type)       ? "$mime_type"       : q{application/octet-stream};
    $custom_meta     = defined($custom_meta)     ? "$custom_meta"     : q{};
    $other_params    = defined($other_params)    ? "$other_params"    : q{};
    $callback_params = defined($callback_params) ? "$callback_params" : q{};

    $prog ||= QBox::UP::new_progress($fsize);

    my $up = QBox::UP->new($self->{client}, $self->{hosts});
    my ($ret, $err) = $up->put($reader_at, $fsize, $prog, $blk_notify, $chk_notify, $notify_params);
    if ($err->{code} != 200) {
        return $ret, $err, $prog;
    }

    my @new_params = ();

    if ($other_params ne q{}) {
        push @new_params, $other_params;
    }
    if ($custom_meta ne q{}) {
        push @new_params, 'meta', qbox_base64_encode_urlsafe($custom_meta);
    }

    my $new_params = join('/', @new_params);
    ($ret, $err) = $up->mkfile(
        'rs-mkfile',
        $entry,
        $mime_type,
        $fsize,
        $new_params,
        $callback_params,
        $prog->{checksums},
        $prog->{blk_count},
    );

    if ($err->{code} != 200) {
        return $ret, $err, $prog;
    }

    return $ret, $err, undef;
} # resumale_put

sub stat {
    my $self   = shift;
    my $bucket = shift;
    my $key    = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key)); 
    my $url = "$self->{hosts}{rs_host}/stat/${encoded_entry}";
    return $self->{client}->call($url, { 'api' => API_STAT });
} # stat

sub publish {
    my $self   = shift;
    my $bucket = shift;
    my $domain = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid domain' } if (not defined($domain));

    $bucket = "$bucket";
    $domain = "$domain";

    my $encoded_domain = qbox_base64_encode_urlsafe($domain); 
    my $url = "$self->{hosts}{rs_host}/publish/${encoded_domain}/from/${bucket}";
    return $self->{client}->call($url, { 'api' => API_PUBLISH });
} # publish

sub unpublish {
    my $self   = shift;
    my $domain = shift;

    return undef, { code => 499, message => 'Invalid domain' } if (not defined($domain));

    $domain = "$domain";

    my $encoded_domain = qbox_base64_encode_urlsafe($domain); 
    my $url = "$self->{hosts}{rs_host}/unpublish/${encoded_domain}";
    return $self->{client}->call($url, { 'api' => API_UNPUBLISH });
} # unpublish

sub delete {
    my $self   = shift;
    my $bucket = shift;
    my $key    = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key)); 
    my $url = "$self->{hosts}{rs_host}/delete/${encoded_entry}";
    return $self->{client}->call($url, { 'api' => API_DELETE });
} # delete

sub drop {
    my $self   = shift;
    my $bucket = shift;

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));

    $bucket = "$bucket";

    my $url = "$self->{hosts}{rs_host}/drop/${bucket}";
    return $self->{client}->call($url, { 'api' => API_DROP });
} # drop

1;

__END__
