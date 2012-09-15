#!/usr/bin/env perl

# ============================================================================
# Name        : QBox::Engine
# Author      : Qiniu Developers
# Version     : 1.0.0.0
# Copyright   : 2012(c) Shanghai Qiniu Information Technologies Co., Ltd.
# Description : 
# ============================================================================

package QBox::Engine;

use strict;
use warnings;

use English;

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

### rs methods
my $rs_get_params = sub {
    my $params = shift;
    my $rs_params = {
        file            => $pickup_param->($params->{src}, $params->{file}, q{}),
        bucket          => $pickup_param->($params->{bkt}, $params->{bucket}),
        key             => $pickup_param->($params->{key}),
        mime            => $pickup_param->($params->{mime}, 'application/octet-stream'),
        meta            => $pickup_param->($params->{meta}),
        params          => $pickup_param->($params->{params}),
        callback_params => $pickup_param->($params->{callback_params}),

        attr            => $pickup_param->($params->{attr}),
        base            => $pickup_param->($params->{base}),
        domain          => $pickup_param->($params->{domain}),
    };
    return $rs_params;
};

### up methods
sub resumable_put {
    my $self   = shift;
    my $params = shift;
    my $notify = $params->{notify} || {};

    my $rs_params = $rs_get_params->($params);

    my $fsize     = (stat($rs_params->{file}))[7];
    my $reader_at = QBox::ReaderAt::File->new($rs_params->{file});

    $notify->{engine} = $self;

    my ($ret, $err, $prog) = ();
    if (defined($notify->{read_prog})) {
        $prog = $notify->{read_prog}->($notify);
    }

    $get_svc->($self, 'rs');
    ($ret, $err, $prog) = $self->{svc}{rs}->resumable_put(
        $prog,
        $notify->{blk_notify},
        $notify->{chk_notify},
        $notify,
        qbox_make_entry($rs_params->{bucket}, $rs_params->{key}),
        $rs_params->{mime},
        $reader_at,
        $fsize,
        $rs_params->{meta},
        $rs_params->{params},
        $rs_params->{callback_params},
    );

    if ($err->{code} != 200) {
        if (defined($notify->{write_prog})) {
            $notify->{write_prog}->($notify, $prog);
        }
    }
    else {
        if (defined($notify->{end_prog})) {
            $notify->{end_prog}->($notify, $prog);
        }
    }

    return $ret, $err;
} # resumable_put

### eu methods
my $eu_gen_settings = sub {
    my $params   = shift;
    my $settings = shift || {};

    my $wms   = $pickup_param->($params->{wms});
    my $names = QBox::EU::wm_setting_names();

    if (defined($wms) and $wms ne q{}) {
        qbox_hash_merge($settings, get_json($wms), 'FROM', $names);
    }
    qbox_hash_merge($settings, $params, 'FROM', $names);

    return $settings;
};

my $exec = undef;
my %methods = (
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

    'get'           => sub { my $self = shift; return $exec->($self, 'rs', 'get', @_); },
    'stat'          => sub { my $self = shift; return $exec->($self, 'rs', 'stat', @_); },
    'publish'       => sub { my $self = shift; return $exec->($self, 'rs', 'publish', @_); },
    'unpublish'     => sub { my $self = shift; return $exec->($self, 'rs', 'unpublish', @_); },
    'put_auth'      => sub { my $self = shift; return $exec->($self, 'rs', 'put_auth', @_); },
    'put_file'      => sub { my $self = shift; return $exec->($self, 'rs', 'put_file', @_); },
    'delete'        => sub { my $self = shift; return $exec->($self, 'rs', 'delete', @_); },
    'drop'          => sub { my $self = shift; return $exec->($self, 'rs', 'drop', @_); },
    'query'         => sub { my $self = shift; return $exec->($self, 'up', 'query', @_); },
    'wmget'         => sub { my $self = shift; return $exec->($self, 'eu', 'wmget', @_); },
    'wmset'         => sub { my $self = shift; return $exec->($self, 'eu', 'wmset', @_); },
    'app_info'      => sub { my $self = shift; return $exec->($self, 'uc', 'app_info', @_); },
    'new_access'    => sub { my $self = shift; return $exec->($self, 'uc', 'new_access', @_); },
    'delete_access' => sub { my $self = shift; return $exec->($self, 'uc', 'delete_access', @_); },
);

# make aliases
$methods{pub}   = $methods{publish};
$methods{unpub} = $methods{unpublish};
$methods{puta}  = $methods{put_auth};
$methods{putaf} = sub { return &put_auth_file; };
$methods{putf}  = $methods{put_file};
$methods{rput}  = sub { return &resumable_put; };
$methods{del}   = $methods{delete};
$methods{appi}  = $methods{app_info};
$methods{nacs}  = $methods{new_access};
$methods{dacs}  = $methods{delete_access};

$exec = sub {
    my $self = shift;
    my $svc  = shift;
    my $cmd  = shift;
    my $args = shift;
    my $opts = shift;

    $get_svc->($self, $svc);
    if ($svc eq 'rs') {
        $args = $rs_get_params->($args);
    }

    my $svc_host = $self->{svc}{$svc};
    return $svc_host->$cmd($args, $opts);
};

our $AUTOLOAD;
sub AUTOLOAD {
    my $nm = $AUTOLOAD;
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
        $method = $sub;
    }
    elsif ($sub eq q{}) {
        $method = sub {
            my ($self, $new) = @_;
            my $old = $self->{$nm};
            if (defined($new)) {
                $self->{$nm} = $new;
            }
            return $old;
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
            return $old;
        };
    }

    if (defined($method)) {
        no strict;
        #*$QBox::Engine::{$nm}{CODE} = $method;
        *$AUTOLOAD = $method;
        use strict;

        goto &$AUTOLOAD;
    }
} # AUTOLOAD

sub wmmod {
    my $self   = shift;
    my $params = shift;

    my ($settings, $err) = $self->wmget($params);
    if ($err->{code} != 200) {
        return undef, $err;
    }

    $settings = $eu_gen_settings->($params, $settings);
    return $self->wmset($settings);
} # wmmod

sub put_auth_file {
    my $self   = shift;
    my $params = shift;

    my ($ret, $err) = $self->put_auth_ex($params);
    return $ret, $err if ($err->{code} != 200);

    my $rs_params = $params;
    my $entry     = qbox_make_entry($rs_params->{bucket}, $rs_params->{key});
    my $mime      = $pickup_param->($rs_params->{mime}, 'application/octet-stream');

    $entry        = qbox_base64_encode_urlsafe($entry);
    $mime         = qbox_base64_encode_urlsafe($mime);

    my $body = {
        action => "/rs-put/${entry}/mimeType/${mime}",
        params => $pickup_param->($rs_params->{params}, q{}),
    };
    
    my $file_body = {
        file => $rs_params->{file},
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

### init methods
sub new {
    my $class = shift || __PACKAGE__;
    my $self  = {
        'svc'   => {},
        'hosts' => {},
        'auth'  => {
            'username'   => undef,
            'password'   => undef,
            'access_key' => undef,
            'secret_key' => undef,
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
} # set_host

sub unset_host {
    my $self  = shift;
    my $hosts = shift;

    if (ref($hosts) eq 'HASH') {
        map { delete($self->{hosts}{$_}) } keys(%$hosts);
        return;
    }

    if (ref($hosts) eq q{}) {
        undef($self->{hosts}{$hosts});
    }
} # unset_host

sub set_auth {
    my $self = shift;
    my $auth = shift;

    if (ref($auth) eq 'HASH') {
        qbox_hash_merge($self->{auth}, $auth, 'TO');
    }
} # set_host

sub unset_auth {
    my $self = shift;
    my $auth = shift;

    if (ref($auth) eq 'HASH') {
        map { delete($self->{auth}{$_}) } keys(%$auth);
        return;
    }

    if (ref($auth) eq q{}) {
        undef($self->{auth}{$auth});
    }
} # unset_host

sub auth_by_password {
    my $self     = shift;
    my $username = shift;
    my $password = shift;

    $username ||= $self->{auth}{username};
    $password ||= $self->{auth}{password};

    if (defined($username) and defined($password)) {
        my $client_id     = $self->{auth}{client_id};
        my $client_secret = $self->{auth}{client_secret};

        my $token = QBox::Auth::Token->new($self->{hosts}, $client_id, $client_secret);
        my $auth  = QBox::Auth::Password->new($token, $username, $password);

        eval {
            my $new_client = QBox::Client->new($auth);

            if ($self->{client}) {
                undef $self->{client};
            }

            $self->{client} = $new_client;
            return 1, q{};
        };

        if ($EVAL_ERROR) {
            return undef, "$EVAL_ERROR";
        }
    }

    return undef, "No username or password.";
} # auth_by_password

sub auth_by_access_key {
    my $self     = shift;
    my $acs_key  = shift;
    my $scr_key  = shift;
    my $policy   = shift;

    my $new_client = undef;

    $acs_key ||= $pickup_param->($self->{auth}{access_key}, 'Put your ACCESS KEY here');
    $scr_key ||= $pickup_param->($self->{auth}{secret_key}, 'Put your SECRET KEY here');
    $policy  ||= $self->{auth}{policy};

    if (not defined($acs_key) or not defined($scr_key)) {
        return undef, "No access key or secret key.";
    }

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
        return undef, "$EVAL_ERROR";
    }

    if ($self->{client}) {
        undef $self->{client};
    }

    $self->{client} = $new_client;
    return 1, q{};
} # auth_by_access_key

sub auto_auth {
    my $self = shift;
    my ($ret, $err) = ();

    ($ret, $err) = $self->auth_by_password();
    return if $ret;

    ($ret, $err) = $self->auth_by_access_key();
    return $ret, $err;
} # auto_auth

1;

__END__
