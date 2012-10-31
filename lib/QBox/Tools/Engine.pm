#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Tools::Engine
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Tools::Engine;

use strict;
use warnings;

use English;
use File::Basename;

use JSON;                            # external library
use Net::Curl::Easy qw(:constants);  # external library

use QBox::Base::Curl;
use QBox::Auth::Password;
use QBox::Auth::Token;
use QBox::Auth::Digest;
use QBox::Auth::UpToken;
use QBox::Auth::Policy;
use QBox::Client;
use QBox::RS;
use QBox::UP;
use QBox::EU;
use QBox::UC;
use QBox::Misc;
use QBox::ReaderAt::File;

my $pickup_param = sub {
    foreach my $p (@_) {
        if (defined($p)) {
            return $p;
        }
    } # foreach
    return undef;
};

my $get_svc = sub {
    my $self = shift;
    my $svc  = shift;
    if (not exists($self->{svc}{$svc})) {
        my $new_svc = lc("new_${svc}");
        $self->{svc}{$svc} = $self->$new_svc();
    }
    return $self->{svc}{$svc};
};

my $prepare_args = sub {
    my $self = shift;
    my $args = shift;
    my $opts = shift;

    my $new_args = undef;
    if (ref($self->{default_args}) eq 'HASH') {
        $new_args = {};
        qbox_hash_merge($new_args, $self->{default_args}, 'FROM');
        qbox_hash_merge($new_args, $args, 'FROM');
    }
    else {
        $new_args = $args;
    }

    my $new_opts = {
        _headers => {},
    };

    if (ref($self->{default_opts}) eq 'HASH') {
        qbox_hash_merge($new_opts, $self->{default_opts}, 'FROM');
    }

    qbox_hash_merge($new_opts->{_headers}, $self->{headers}, 'FROM');
    qbox_hash_merge($new_opts, $opts, 'FROM');

    return $new_args, $new_opts;
};

### rs methods
my $rs_pickup_args = sub {
    my $args = shift;
    my $rs_args = {
        file            => $pickup_param->($args->{file}, $args->{src}),
        bucket          => $pickup_param->($args->{bucket}, $args->{bkt}),
        key             => $pickup_param->($args->{key}),
        mime_type       => $pickup_param->($args->{mime_type}, $args->{mime}, 'application/octet-stream'),
        custom_meta     => $pickup_param->($args->{meta}),
        params          => $pickup_param->($args->{params}),
        callback_params => $pickup_param->($args->{callback_params}),

        expires_in      => $pickup_param->($args->{expires_in}),

        attr            => $pickup_param->($args->{attr}),
        base            => $pickup_param->($args->{base}),
        domain          => $pickup_param->($args->{domain}),

        uptoken         => $pickup_param->($args->{uptoken}),
    };

    $rs_args->{key} ||= (defined($rs_args->{file})) ? basename($rs_args->{file}) : undef;

    return $rs_args;
};

sub putaf {
    return &put_auth_file;
} # putaf

sub put_auth_file {
    my $self = shift;
    my $args = shift;
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);

    my ($ret, $err) = $self->put_auth_ex($new_args);
    return $ret, $err if ($err->{code} != 200);

    my $rs_args = $rs_pickup_args->($new_args);
    my $entry   = qbox_make_entry($rs_args->{bucket}, $rs_args->{key});
    my $mime    = $pickup_param->($rs_args->{mime}, 'application/octet-stream');

    $entry      = qbox_base64_encode_urlsafe($entry);
    $mime       = qbox_base64_encode_urlsafe($mime);

    my $body = {
        action => "/rs-put/${entry}/mimeType/${mime}",
        params => $pickup_param->($rs_args->{params}, q{}),
    };
    
    my $file_body = {
        file => $rs_args->{file},
    };

    my $form = qbox_curl_make_multipart_form($body, $file_body);
    my $curl = qbox_curl_call_pre(
        $ret->{url},
        undef,
        { 'api' => 'rs.put-auth-file' }
    );
    $curl->setopt(CURLOPT_HTTPPOST, $form);
    return qbox_curl_call_core($curl);
} # put_auth_file

sub upload_file {
    my $self = shift;
    my $args = shift;
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);
    $new_args->{uptoken} = $self->{client}{auth}->gen_uptoken();
    return $self->upload($new_args, $new_opts);
} # upload_file

### up methods
my $prepare_for_resumable_put = sub {
    my $self = shift;
    my $args = shift;
    my $opts = shift;

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);
    my $rs_args = $rs_pickup_args->($new_args);

    $new_args->{fsize} ||= (stat($rs_args->{file}))[7];

    my $notify = $new_opts->{_notify} || {};
    $notify->{engine}  = $self;

    if (defined($notify->{read_prog})) {
        $new_args->{prog} ||= $notify->{read_prog}->($notify);
    }
    else {
        $new_args->{prog} ||= QBox::UP::new_progress($new_args->{fsize});
    }

    return $new_args, $new_opts, $rs_args;
};

my $cleanup_for_resumable_put = sub {
    my $self = shift;
    my $args = shift;
    my $opts = shift;
    my $err  = shift;

    my $notify = $opts->{_notify} || {};

    if ($err->{code} != 200) {
        if (defined($notify->{write_prog})) {
            $notify->{write_prog}->($notify, $args->{prog});
        }
    }
    else {
        if (defined($notify->{end_prog})) {
            $notify->{end_prog}->($notify, $args->{prog});
        }
    }

    if ($args->{reader_at}) {
        $args->{reader_at}->close();
    }
};

sub rbput {
    return &resumable_blockput;
} # rbput

sub resumable_blockput {
    my $self = shift;
    my $args = shift;
    my $opts = shift || {};

    my ($new_args, $new_opts, $rs_args) = $prepare_for_resumable_put->($self, $args, $opts);
    $new_args->{reader_at} = QBox::ReaderAt::File->new($rs_args->{file});

    my $blk_index = $new_args->{blk_index};
    my $blk_prog  = $new_args->{prog}{progs}[$blk_index];

    $get_svc->($self, 'up');
    my ($ret, $err) = $self->{svc}{up}->resumable_blockput(
        $new_args->{reader_at},
        $blk_index,
        $blk_prog->{offset} + $blk_prog->{rest_size},
        $new_args->{chk_size} || QBox::Config::QBOX_PUT_CHUNK_SIZE,
        $new_args->{retry_times} || QBox::Config::QBOX_PUT_RETRY_TIMES,
        $blk_prog,
        $new_opts->{_notify}{chk_notify},
        $new_opts->{_notify},
        $new_opts,
    );

    $cleanup_for_resumable_put->($self, $new_args, $new_opts, $err);
    return $ret, $err;
} # resumable_put_blockput

sub mkfile {
    my $self = shift;
    my $args = shift;
    my $opts = shift || {};

    my ($new_args, $new_opts, $rs_args) = $prepare_for_resumable_put->($self, $args, $opts);

    if (ref($new_args->{ctx}) eq 'ARRAY' ) {
        for (my $i = 0; $i < scalar(@{$new_args->{ctx}}); ++$i) {
            $new_args->{prog}{progs}[$i]{ctx} = $new_args->{ctx}[$i];
        } # for
    }

    $get_svc->($self, 'up');
    my ($ret, $err) = $self->{svc}{up}->mkfile(
        $new_args->{cmd} || 'rs-mkfile',
        $rs_args->{bucket},
        $rs_args->{key},
        $new_args->{mime_type},
        $new_args->{fsize},
        $new_args->{params},
        $new_args->{callback_params},
        $new_args->{prog},
        $new_opts,
    );

    $cleanup_for_resumable_put->($self, $new_args, $new_opts, $err);
    return $ret, $err;
} # mkfile

sub rput {
    return &resumable_put;
} # rput

sub resumable_put {
    my $self = shift;
    my $args = shift;
    my $opts = shift || {};

    my ($new_args, $new_opts, $rs_args) = $prepare_for_resumable_put->($self, $args, $opts);
    $new_args->{reader_at} = QBox::ReaderAt::File->new($rs_args->{file});

    my $ret = undef;
    my $err = undef;

    $get_svc->($self, 'rs');
    ($ret, $err, $new_args->{prog}) = $self->{svc}{rs}->resumable_put(
        $new_args->{prog},
        $new_opts->{_notify}->{blk_notify},
        $new_opts->{_notify}->{chk_notify},
        $new_opts->{_notify},
        $rs_args->{bucket},
        $rs_args->{key},
        $rs_args->{mime_type},
        $new_args->{reader_at},
        $new_args->{fsize},
        $rs_args->{custom_meta},
        $rs_args->{params},
        $rs_args->{callback_params},
        $new_opts,
    );

    $cleanup_for_resumable_put->($self, $new_args, $new_opts, $err);
    return $ret, $err;
} # resumable_put

### eu methods
my $eu_gen_settings = sub {
    my $args   = shift;
    my $settings = shift || {};

    my $wms   = $pickup_param->($args->{wms});
    my $names = QBox::EU::wm_setting_names();

    if (defined($wms) and $wms ne q{}) {
        qbox_hash_merge($settings, qbox_json_load($wms), 'FROM', $names);
    }
    qbox_hash_merge($settings, $args, 'FROM', $names);

    return $settings;
};

sub wmmod {
    my $self = shift;
    my $args = shift;
    my $opts = shift;

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);

    my ($settings, $err) = $self->wmget($new_args);
    if ($err->{code} != 200) {
        return undef, $err;
    }

    $settings = $eu_gen_settings->($new_args, $settings);
    return $self->wmset($settings);
} # wmmod

### general methods
my $rs_exec = sub {
    my $self = shift;
    my $cmd  = shift;
    my $args = shift || {};
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);
    $new_args = $rs_pickup_args->($new_args);

    $get_svc->($self, 'rs');
    my $svc_host = $self->{svc}{rs};
    return $svc_host->$cmd($new_args, $new_opts);
};

my $up_exec = sub {
    my $self = shift;
    my $cmd  = shift;
    my $args = shift || {};
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);

    $get_svc->($self, 'up');
    my $svc_host = $self->{svc}{up};
    return $svc_host->$cmd($new_args, $new_opts);
};

my $uc_exec = sub {
    my $self = shift;
    my $cmd  = shift;
    my $args = shift || {};
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);

    $get_svc->($self, 'uc');
    my $svc_host = $self->{svc}{uc};
    return $svc_host->$cmd($new_args, $new_opts);
};

my $eu_exec = sub {
    my $self = shift;
    my $cmd  = shift;
    my $args = shift || {};
    my $opts = shift || {};

    my ($new_args, $new_opts) = $prepare_args->($self, $args, $opts);

    $get_svc->($self, 'eu');
    my $svc_host = $self->{svc}{eu};
    return $svc_host->$cmd($new_args, $new_opts);
};

my %methods = (
    'default_args'  => '',
    'default_opts'  => '',

    'auth'          => '',
    'access_key'    => 'auth',
    'secret_key'    => 'auth',
    'client_id'     => 'auth',
    'client_secret' => 'auth',
    'username'      => 'auth',
    'password'      => 'auth',
    'policy'        => 'auth',

    'hosts'         => '',
    'ac_host'       => 'hosts',
    'io_host'       => 'hosts',
    'up_host'       => 'hosts',
    'rs_host'       => 'hosts',
    'uc_host'       => 'hosts',
    'eu_host'       => 'hosts',

    'get'           => $rs_exec,
    'stat'          => $rs_exec,
    'publish'       => $rs_exec,
    'unpublish'     => $rs_exec,
    'put_auth'      => $rs_exec,
    'put_auth_ex'   => $rs_exec,
    'put_file'      => $rs_exec,
    'delete'        => $rs_exec,
    'drop'          => $rs_exec,
    'upload'        => $rs_exec,

    'query'         => $up_exec,
    'mkblock'       => $up_exec,
    'blockput'      => $up_exec,

    'wmget'         => $eu_exec,
    'wmset'         => $eu_exec,

    'app_info'      => $uc_exec,
    'new_access'    => $uc_exec,
    'delete_access' => $uc_exec,
);

# make aliases
$methods{pub}   = sub { $_[1] = 'publish';       return &$rs_exec; };
$methods{unpub} = sub { $_[1] = 'unpublish';     return &$rs_exec; };
$methods{puta}  = sub { $_[1] = 'put_auth';      return &$rs_exec; };
$methods{putf}  = sub { $_[1] = 'put_file';      return &$rs_exec; };
$methods{del}   = sub { $_[1] = 'delete';        return &$rs_exec; };

$methods{appi}  = sub { $_[1] = 'app_info';      return &$uc_exec; };
$methods{nacs}  = sub { $_[1] = 'new_access';    return &$uc_exec; };
$methods{dacs}  = sub { $_[1] = 'delete_access'; return &$uc_exec; };

sub AUTOLOAD {
    my $nm = our $AUTOLOAD;
    $nm =~ s/^.+://;

    if (not exists($methods{$nm})) {
        return undef, {
            'code'    => 499,
            'message' => "No such command.(cmd=${nm})",
        };
    }

    my $method = undef;
    my $sub = $methods{$nm};
    if (ref($sub) eq 'CODE') {
        $method = sub { splice(@_, 1, 0, $nm); return &$sub; };
    }
    elsif ($sub eq q{}) {
        $method = sub {
            my ($self, $new) = @_;
            my $old = $self->{$nm};
            if (defined($new)) {
                $self->{$nm} = $new;
            }
            return { 'old' => $old, 'new' => $new }, { 'code' => 200, 'message' => 'OK' };
        };
    }
    elsif ($sub ne q{}) {
        $method = sub {
            my ($self, $new) = @_;
            $self->{$sub} ||= {};
            my $old = $self->{$sub}{$nm};
            if (defined($new)) {
                $self->{$sub}{$nm} = $new;
            }
            return { 'old' => $old, 'new' => $new }, { 'code' => 200, 'message' => 'OK' };
        };
    }

    if (defined($method)) {
        no strict;
        *$AUTOLOAD = $method;
        use strict;

        goto &$AUTOLOAD;
    }
} # AUTOLOAD

### init methods
sub new {
    my $class = shift || __PACKAGE__;
    my $self  = {
        'svc'     => {},
        'hosts'   => {},
        'headers' => {},
        'auth'    => {
            'client_id'     => undef,
            'client_secret' => undef,

            'username'      => undef,
            'password'      => undef,

            'access_key'    => undef,
            'secret_key'    => undef,

            'policy'        => undef,
        },
        'out_fh' => undef,
    };
    return bless $self, $class;
} # new

### helper methods
sub new_up {
    my $self = shift;
    return QBox::UP->new($self->{client}, $self->{hosts});
} # new_up

sub new_rs {
    my $self = shift;
    return QBox::RS->new($self->{client}, $self->{hosts});
} # new_rs

sub new_uc {
    my $self = shift;
    return QBox::UC->new($self->{client}, $self->{hosts});
} # new_uc

sub new_eu {
    my $self = shift;
    return QBox::EU->new($self->{client}, $self->{hosts});
} # new_eu

sub set_host {
    my $self  = shift;
    my $hosts = shift;
    my $value = shift;

    if (ref($hosts) eq 'HASH') {
        qbox_hash_merge($self->{hosts}, $hosts, 'FROM');
    }

    return {}, { 'code' => 200, 'message' => 'Host info set' };
} # set_host

sub unset_host {
    my $self  = shift;
    my $hosts = shift;

    if (ref($hosts) eq 'HASH') {
        map { delete($self->{hosts}{$_}) } keys(%$hosts);
    }
    elsif (ref($hosts) eq q{}) {
        undef($self->{hosts}{$hosts});
    }

    return {}, { 'code' => 200, 'message' => 'Host info unset' };
} # unset_host

sub set_auth {
    my $self = shift;
    my $auth = shift;

    if (ref($auth) eq 'HASH') {
        qbox_hash_merge($self->{auth}, $auth, 'TO');
    }

    return {}, { 'code' => 200, 'message' => 'Auth info set'};
} # set_host

sub unset_auth {
    my $self = shift;
    my $auth = shift;

    if (ref($auth) eq 'HASH') {
        map { delete($self->{auth}{$_}) } keys(%$auth);
    }
    elsif (ref($auth) eq q{}) {
        undef($self->{auth}{$auth});
    }

    return {}, { 'code' => 200, 'message' => 'Auth info unset'};
} # unset_host

sub auth_by_password {
    my $self = shift;
    my $args = shift;

    my $username = $pickup_param->($args->{username}, $self->{auth}{username});
    my $password = $pickup_param->($args->{password}, $self->{auth}{password});

    if (defined($username) and defined($password)) {
        my $client_id     = $self->{auth}{client_id};
        my $client_secret = $self->{auth}{client_secret};

        my $token = QBox::Auth::Token->new($self->{hosts}, $client_id, $client_secret);
        my $auth  = QBox::Auth::Password->new($token, $username, $password);

        eval {
            my $new_client = QBox::Client->new($auth);

            if ($self->authorized()) {
                $self->unauth();
            }

            $self->{client} = $new_client;
            return {}, { 'code' => 200, 'message' => 'Login by password'};
        };

        if ($EVAL_ERROR) {
            return undef, { 'code' => 499, 'message' => "$EVAL_ERROR" };
        }
    }

    return undef, { 'code' => 499, 'message' => "No username or password" };
} # auth_by_password

sub auth_by_access_key {
    my $self = shift;
    my $args = shift;

    my $acs_key = $pickup_param->($args->{access_key}, $self->{auth}{access_key}, 'Put your ACCESS KEY here');
    my $scr_key = $pickup_param->($args->{secret_key}, $self->{auth}{secret_key}, 'Put your SECRET KEY here');
    my $policy  = $pickup_param->($args->{policy}, $self->{auth}{policy});

    if (not defined($acs_key) or not defined($scr_key)) {
        return undef, { 'code' => 499, 'message' => "No access key or secret key." };
    }

    my $new_client = undef;
    eval {
        if (defined($policy) and $policy ne q{}) {
            $policy = ref($policy) eq q{} ? from_json($policy) : $policy;
            $policy = QBox::Auth::Policy->new($policy);
            my $auth = QBox::Auth::UpToken->new($acs_key, $scr_key, $policy);
            $new_client = QBox::Client->new($auth);
        }
        else {
            my $auth = QBox::Auth::Digest->new($acs_key, $scr_key);
            $new_client = QBox::Client->new($auth);
        }
    };

    if ($EVAL_ERROR) {
        return undef, { 'code' => 499, 'message' => "$EVAL_ERROR" };
    }

    if ($self->authorized()) {
        $self->unauth();
    }

    $self->{client} = $new_client;
    return {}, { 'code' => 200, 'message' => 'Login by access key'};
} # auth_by_access_key

sub authorized {
    my $self = shift;
    return defined($self->{client});
} # authorized

sub unauth {
    my $self = shift;
    undef $self->{client};
} # unauth

sub auto_auth {
    my $self = shift;
    my ($ret, $err) = ();

    ($ret, $err) = $self->auth_by_password();
    return $ret, $err if $ret;

    ($ret, $err) = $self->auth_by_access_key();
    return $ret, $err;
} # auto_auth

sub set_header {
    my $self    = shift;
    my $headers = shift;
    my $value   = shift;

    if (ref($headers) eq 'HASH') {
        qbox_hash_merge($self->{headers}, $headers, 'FROM');
    }
    elsif (ref($headers) eq q{}) {
        $self->{headers}{$headers} = $value;
    }

    return {}, { 'code' => 200, 'message' => 'Header info set' };
} # set_header

sub unset_header {
    my $self  = shift;
    my $headers = shift;

    if (ref($headers) eq 'HASH') {
        map { delete($self->{headers}{$_}) } keys(%$headers);
    }
    elsif (ref($headers) eq q{}) {
        undef($self->{headers}{$headers});
    }

    return {}, { 'code' => 200, 'message' => 'Header info unset' };
} # unset_header

1;

__END__
