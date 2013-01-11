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
use QBox::Stub;
use QBox::Misc;

use constant API_GET         => 'rs.get';
use constant API_PUT         => 'rs.put';
use constant API_PUT_AUTH_EX => 'rs.put-auth-ex';
use constant API_STAT        => 'rs.stat';
use constant API_PUBLISH     => 'rs.publish';
use constant API_UNPUBLISH   => 'rs.unpublish';
use constant API_DELETE      => 'rs.delete';
use constant API_DROP        => 'rs.drop';

my @TRANSMIT = qw(
    qbox_rs_new

    qbox_rs_get
    qbox_rs_get_if_not_modified

    qbox_rs_put
    qbox_rs_put_file
    qbox_rs_resumable_put
    qbox_rs_put_auth
    qbox_rs_put_auth_ex

    qbox_rs_upload

    qbox_rs_stat
    qbox_rs_delete
    qbox_rs_drop
);

our @ISA = qw(Exporter);
our @EXPORT = (
    @TRANSMIT,
);

### for procedures
no strict;

foreach my $sub_nm (@TRANSMIT) {
    my $trans = $sub_nm;
    $trans =~ s/^qbox_rs_//;
    *{"QBox::RS::$sub_nm"} = sub { return &{*{"QBox::RS::$trans"}{CODE}} };
} # foreach

use strict;

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
    my $self = shift;
    my ($bucket, $key, $attr, $base, $opts) =
        qbox_extract_args([qw{bucket key attr base}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key));
    my @args = (
        $self->{hosts}{rs_host},
        'get' => $encoded_entry,
    );

    if (defined($base) and "$base" ne q{}) {
        push @args, 'base', "$base";
    }

    if (defined($attr) and "$attr" ne q{}) {
        $attr = qbox_base64_encode_urlsafe("$attr");
        push @args, 'attName', $attr;
    }

    $opts ||= {};
    $opts->{_api} = API_GET;
    my $url = join('/', @args);
    return $self->{client}->call($url, $opts);
} # get

sub get_if_not_modified {
    return &get;
} # get_if_not_modified

sub put {
    my $self = shift;
    my ($bucket, $key, $mime_type, $reader, $fsize, $custom_meta, $opts) =
        qbox_extract_args([qw{bucket key mime_type reader fsize custom_meta}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket      = "$bucket";
    $key         = "$key";
    $mime_type   = defined($mime_type) ? "$mime_type" : q{application/octet-stream};

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key));
    $mime_type = qbox_base64_encode_urlsafe($mime_type);

    my @args = (
        $self->{hosts}{up_host},
        'rs-put'   => $encoded_entry,
        'mimeType' => $mime_type,
    );

    if (defined($custom_meta) and "$custom_meta" ne q{}) {
        $custom_meta = qbox_base64_encode_urlsafe("$custom_meta");
        push @args, 'meta', $custom_meta;
    }

    $opts ||= {};
    $opts->{_api} = API_PUT;
    my $url = join('/', @args);
    return $self->{client}->call_with_binary($url, $reader, $fsize, $opts);
} # put

sub put_file {
    my $self = shift;
    my ($bucket, $key, $mime_type, $file, $custom_meta, $opts) =
        qbox_extract_args([qw{bucket key mime_type file custom_meta}], @_);

    return undef, { code => 499, message => 'Invalid file' } if (not defined($file));

    $file = "$file";
    return undef, { code => 499, message => 'Cannot read file' } if (not -r $file);

    my $fsize  = (stat($file))[7];
    my $reader = QBox::Reader::File->new({ file => $file });

    # forward invocation
    return $self->put($bucket, $key, $mime_type, $reader, $fsize, $custom_meta, $opts);
} # put_file

# may be deprecated
sub put_auth {
    my $self       = shift;
    my $expires_in = shift || 10;
    return $self->put_auth_ex($expires_in, @_);
} # put_auth

# may be deprecated
sub put_auth_ex {
    my $self = shift;
    my ($expires_in, $callback, $opts) =
        qbox_extract_args([qw{expires_in callback}], @_);

    return undef, { code => 499, message => 'Invalid expiry' } if (not defined($expires_in));

    my @args = (
        $self->{hosts}{io_host},
        'put-auth' => "$expires_in",
    );

    if ($callback) {
        $callback = qbox_base64_encode_urlsafe($callback);
        push @args, 'callback', $callback;
    }

    $opts ||= {};
    $opts->{_api} = API_PUT_AUTH_EX;
    my $url = join('/', @args);
    return $self->{client}->call($url, $opts);
} # put_auth_ex

sub resumable_put {
    my $self = shift;
    my ($prog, $blk_notify, $chk_notify, $notify_params,
        $bucket, $key, $mime_type, $reader_at, $fsize,
        $custom_meta, $params, $callback_params, $opts) =
        qbox_extract_args([qw{
        prog blk_notify chk_notify notify_params
        bucket key mime_type reader_at fsize
        custom_meta params callback_params}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));
    return undef, { code => 499, message => 'Invalid file size' } if (not defined($fsize));

    $bucket    = "$bucket";
    $key       = "$key";
    $mime_type = defined($mime_type) ? "$mime_type" : q{application/octet-stream};

    $prog ||= QBox::UP::new_progress($fsize);

    my $up = QBox::UP->new($self->{client}, $self->{hosts});
    my ($ret, $err) = $up->put_blocks_one_by_one(
        $reader_at,
        $fsize,
        $prog,
        $blk_notify,
        $chk_notify,
        $notify_params,
        $opts
    );
    return $ret, $err, $prog if ($err->{code} != 200);

    my @new_params = ();
    if (defined($params) and "$params" ne q{}) {
        push @new_params, "$params";
    }
    if (defined($custom_meta) and "$custom_meta" ne q{}) {
        push @new_params, 'meta', qbox_base64_encode_urlsafe("$custom_meta");
    }

    my $new_params = join('/', @new_params);
    ($ret, $err) = $up->mkfile(
        'rs-mkfile',
        $bucket,
        $key,
        $mime_type,
        $fsize,
        $new_params,
        $callback_params,
        $prog,
        $opts
    );
    return $ret, $err, $prog if ($err->{code} != 200);

    return $ret, $err, undef;
} # resumable_put

sub stat {
    my $self = shift;
    my ($bucket, $key, $opts) = qbox_extract_args([qw{bucket key}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    $opts ||= {};
    $opts->{_api} = API_STAT;
    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key)); 
    my $url = "$self->{hosts}{rs_host}/stat/${encoded_entry}";
    return $self->{client}->call($url, $opts);
} # stat

sub publish {
    my $self = shift;
    my ($bucket, $domain, $opts) = qbox_extract_args([qw{bucket domain}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid domain' } if (not defined($domain));

    $bucket = "$bucket";
    $domain = "$domain";

    $opts ||= {};
    $opts->{_api} = API_PUBLISH;
    my $encoded_domain = qbox_base64_encode_urlsafe($domain); 
    my $url = "$self->{hosts}{rs_host}/publish/${encoded_domain}/from/${bucket}";
    return $self->{client}->call($url, $opts);
} # publish

sub unpublish {
    my $self = shift;
    my ($domain, $opts) = qbox_extract_args([qw{domain}], @_);

    return undef, { code => 499, message => 'Invalid domain' } if (not defined($domain));

    $domain = "$domain";

    $opts ||= {};
    $opts->{_api} = API_UNPUBLISH;
    my $encoded_domain = qbox_base64_encode_urlsafe($domain); 
    my $url = "$self->{hosts}{rs_host}/unpublish/${encoded_domain}";
    return $self->{client}->call($url, $opts);
} # unpublish

sub delete {
    my $self = shift;
    my ($bucket, $key, $opts) = qbox_extract_args([qw{bucket key}], @_);

    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));
    return undef, { code => 499, message => 'Invalid key' } if (not defined($key));

    $bucket = "$bucket";
    $key    = "$key";

    $opts ||= {};
    $opts->{_api} = API_DELETE;
    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key)); 
    my $url = "$self->{hosts}{rs_host}/delete/${encoded_entry}";
    return $self->{client}->call($url, $opts);
} # delete

sub drop {
    my $self = shift;
    my ($bucket, $opts) = qbox_extract_args([qw{bucket}], @_);

    return undef, { code => 699, message => 'Temporarily Unavailable' };
    return undef, { code => 499, message => 'Invalid bucket' } if (not defined($bucket));

    $bucket = "$bucket";

    $opts ||= {};
    $opts->{_api} = API_DROP;
    my $url = "$self->{hosts}{rs_host}/drop/${bucket}";
    return $self->{client}->call($url, $opts);
} # drop

sub upload {
    my $self = shift;
    my ($bucket, $key, $file, $mime_type, $custom_meta, $callback_params, $uptoken, $opts) =
        qbox_extract_args([qw(bucket key file mime_type custom_meta callback_params uptoken)], @_);

    $bucket = "$bucket";
    $key    = "$key";

    my $encoded_entry = qbox_base64_encode_urlsafe(qbox_make_entry($bucket, $key)); 

    $mime_type = defined($mime_type) ? "$mime_type" : q{application/octet-stream};
    my $encoded_mime_type = qbox_base64_encode_urlsafe($mime_type);

    $uptoken = defined($uptoken) ? $uptoken : $self->{client}{auth}->gen_uptoken();

    my @action = (
        '/rs-put',
        $encoded_entry,
        'mime_type' => $encoded_mime_type,
    );

    if (defined($custom_meta) and "$custom_meta" ne q{}) {
        $custom_meta = qbox_base64_encode_urlsafe("$custom_meta");
        push @action, 'meta', $custom_meta;
    }

    my $action = qbox_url_append_params(join(q{/}, @action), $opts, '_action_params');
    QBox::Stub::call_stub("rs.upload.action", \$action);

    my $url = "$self->{hosts}{up_host}/upload";
    my $body = {
        'action' => "$action",
        'auth'   => "$uptoken",
    };

    my $file_body = {
        'file'   => "$file",
    };

    if (defined($callback_params)) {
        $body->{params} = "$callback_params";
    }

    $opts->{_api} = 'rs.upload';
    return $self->{client}->call_with_multipart_form($url, [$body, $file_body], undef, $opts);
} # upload

1;

__END__
