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
    };
    return bless $self, $class;
} # new


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
$methods{putaf} = $methods{put_auth_file};
$methods{putf}  = $methods{put_file};
$methods{rput}  = $methods{resumable_put};
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

    if (not exists($self->{svc}{$svc})) {
        my $new_svc = lc("new_${svc}");
        $self->{svc}{$svc} = $self->$new_svc->();
    }

    my $method = $methods{$cmd};
    if (not defined($method)) {
        return undef, { code => 499, message => "Unknown command '$cmd'" };
    }

    my $svc_host = $self->{svc}{$svc};
    return $svc_host->$method->($args, $opts);
};

our $AUTOLOAD;
sub AUTOLOAD {
    my $nm = $AUTOLOAD;
    $nm =~ s/^.+://;

    return if (not exists($methods{$nm}));

    my $method = undef;
    my $sub = $methods{$nm};
    if ($sub eq q{}) {
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
        *$QBox::Engine::{$nm}{CODE} = $method;
        use strict;

        return &$method;
    }
} # AUTOLOAD

sub set_host {
    my $self  = shift;
    my $hosts = shift;
    my $value = shift;

    if (ref($hosts) eq 'HASH') {
        qbox_merge_hash($self->{hosts}, $hosts);
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
        qbox_hash_merge($self->{auth}, $auth, keys(%{$self->{auth}}));
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
    $policy  ||= $pickup_param->($self->{auth}{policy}, 'Put your POLICY here');

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

    #'put_auth_file' => sub { my $self = shift; return $exec->($self, 'rs', 'put_auth_file', @_); },

### up
my $up_read_progress_as_plain_text = sub {
    my $fh = shift;

    my $prog = {};
    my $line = undef;

    $line = <$fh>;
    if ($line !~ m/^block_count=(\d+)\n/) {
        die "Invalid progress file: No block count.";
    }
    $prog->{blk_count} = $1;

    $prog->{checksums} = [];
    for (my $i = 0; $i < $prog->{blk_count}; ++$i) {
        $line = <$fh>;
        if ($line !~ m/^checksum=(.*)\n/) {
            die "Invalid progress file: Invalid checksum.";
        }

        push @{$prog->{checksums}}, { value => $1 };
    } # for

    $prog->{progs} = [];
    for (my $i = 0; $i < $prog->{blk_count}; ++$i) {
        my $pg = {};

        $line = <$fh>;
        if ($line !~ m/^offset=(\d+)\n/) {
            die "Invalid progress file: Invalid offset.";
        }
        $pg->{offset} = $1;

        $line = <$fh>;
        if ($line !~ m/^rest_size=(\d+)\n/) {
            die "Invalid progress file: Invalid rest size.";
        }
        $pg->{rest_size} = $1;

        $line = <$fh>;
        if ($line !~ m/^ctx=(.*)\n/) {
            die "Invalid progress file: Invalid context.";
        }
        $pg->{ctx} = $1;

        push @{$prog->{progs}}, $pg;
    } # for

    return $prog;
};

my $up_read_progress = sub {
    my $params = shift;
    
    my $prog_fl = $params->{prog_fl};
    return undef if (not defined($prog_fl) or $prog_fl eq q{});
    return undef if (not -r $prog_fl);

    open(my $fh, '<', $prog_fl) or die "$OS_ERROR";

    my $prog = undef;
    if ($prog_fl =~ m/json$/i) {
        local $/ = undef;
        my $text = <$fh>;
        $prog = from_json($text);
    }
    else {
        $prog = $up_read_progress_as_plain_text->($fh);
    }

    close($fh);
    return $prog;
};

my $up_up_write_progress_as_plain_text = sub {
    my $fh   = shift;
    my $prog = shift;

    printf {$fh} "block_count=%d\n", $prog->{blk_count};

    foreach my $cksum (@{$prog->{checksums}}) {
        printf {$fh} "checksum=%s\n", ($cksum->{value} || q{});
    } # foreach

    foreach my $pg (@{$prog->{progs}}) {
        printf {$fh} "offset=%d\n", $pg->{offset};
        printf {$fh} "rest_size=%d\n", $pg->{rest_size};
        printf {$fh} "ctx=%s\n", ($pg->{ctx} || q{});
    } # foreach
};

my $up_write_progress = sub {
    my $params = shift;
    my $prog   = shift;

    my $prog_fl = $params->{prog_fl};
    return if (not defined($prog_fl) or $prog_fl eq q{});

    open(my $fh, '>', $prog_fl) or die "$OS_ERROR";

    if ($prog_fl =~ m/json$/i) {
        printf {$fh} "%s", to_json($prog, { pretty => 1 });
    }
    else {
        $up_up_write_progress_as_plain_text->($fh, $prog);
    }

    close($fh);
};

my $up_blk_abort = sub {
    my $params    = shift;
    my $blk_index = shift;
    my $checksum  = shift;

    my $stop_idx = $params->{stop_idx};
    if (defined($stop_idx) and $blk_index == $stop_idx) {
        print "Abort uploading block(#${stop_idx}).\n";
        return 0;
    }
    return 1;
};

my $up_blk_notify = sub {
    my $params    = shift;
    my $blk_index = shift;
    my $checksum  = shift;

    printf "blk_index=%d, checksum=[%s]\n", $blk_index, $checksum->{value};
    $up_blk_abort->($params, $blk_index, $checksum);
};

my $up_chk_abort = sub {
    my $params    = shift;
    my $blk_index = shift;
    my $prog      = shift;

    my $stop_idx = $params->{stop_idx};
    if (defined($stop_idx) and $blk_index == $stop_idx) {
        my $stop_size = $params->{stop_size};
        if (defined($stop_size) and $prog->{offset} >= $stop_size) {
            print "Abort uploading chunk(#$prog->{stop_idx}, \@$prog->{offset}).\n";
            return 0;
        }
    }
    return 1;
};

my $up_chk_notify = sub {
    my $params    = shift;
    my $blk_index = shift;
    my $prog      = shift;

    printf "blk_index=%d, uploaded=%d, rest=%d, ctx=[%s]\n",
        $blk_index, $prog->{offset}, $prog->{rest_size}, $prog->{ctx};
    $up_chk_abort->($params, $blk_index, $prog);
};

sub resumable_put {
    my $self   = shift;
    my $params = shift;

    my $rs_params = $params;

    my $fsize     = (stat($rs_params->{src}))[7];
    my $reader_at = QBox::ReaderAt::File->new($rs_params->{src});

    my $notify_blk = $pickup_param->($params->{notify_blk});
    my $notify_chk = $pickup_param->($params->{notify_chk});

    my $notify_params = {};
    $notify_params->{stop_idx}  = $pickup_param->($params->{stop_idx});
    $notify_params->{stop_size} = $pickup_param->($params->{stop_size});
    $notify_params->{prog_fl}   = $pickup_param->($params->{prog_fl});

    if (defined($notify_params->{stop_size})) {
        $notify_params->{stop_idx} ||= 0;
    }

    my $ret  = undef;
    my $err  = undef;
    my $prog = $up_read_progress->($notify_params);

    ($ret, $err, $prog) = $self->{svc}{rs}->resumale_put(
        $prog,
        defined($notify_blk) ? $up_blk_notify : $up_blk_abort,
        defined($notify_chk) ? $up_chk_notify : $up_chk_abort,
        $notify_params,
        qbox_make_entry($rs_params->{bucket}, $rs_params->{key}),
        $rs_params->{mime},
        $reader_at,
        $fsize,
        $rs_params->{meta},
        $rs_params->{params},
        $rs_params->{callback_params},
    );

    if ($err->{code} != 200) {
        $up_write_progress->($notify_params, $prog);
    }
    elsif ($notify_params->{prog_fl} and -w $notify_params->{prog_fl}) {
        unlink($notify_params->{prog_fl});
    }

    return $ret, $err;
} # resumable_put

### eu
my $eu_gen_settings = sub {
    my $params   = shift;
    my $settings = shift || {};

    my $wms   = $pickup_param->($params->{wms});
    my $names = QBox::EU::wm_setting_names();

    #qbox_hash_merge($settings, $conf, $names);
    if (defined($wms) and $wms ne q{}) {
        qbox_hash_merge($settings, get_json($wms), $names);
    }
    qbox_hash_merge($settings, $params, $names);

    return $settings;
};

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
        file => $rs_params->{src},
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

1;

__END__
