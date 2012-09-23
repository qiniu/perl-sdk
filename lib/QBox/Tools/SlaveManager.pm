#!/usr/bin/env perl

package QBox::Tools::SlaveManager;

use strict;
use warnings;

my $renew = sub {
    my $self = shift;

    foreach my $proc (values(%{$self->{procs}})) {
        close($proc->{mgr_read});
        close($proc->{mgr_write});
        close($proc->{mgr_err});
    } # foreach

    $self->{rfds}  = "";
    $self->{count} = 0;
    $self->{busy}  = 0;
    $self->{procs} = {};
};

my $fork = sub {
    pipe my $work_read, my $mgr_write;
    pipe my $mgr_read,  my $work_write;
    pipe my $mgr_err,   my $work_err;

    my $pid = fork;
    if (not defined($pid)) {
        # failed
        return undef;
    }

    if ($pid > 0) {
        # parent
        close($work_read);
        close($work_write);
        close($work_err);
        return $pid, {
            'pid'       => $pid,
            'mgr_read'  => $mgr_read,
            'mgr_write' => $mgr_write,
            'mgr_err'   => $mgr_err,
        };
    }

    # child
    close($mgr_read);
    close($mgr_write);
    close($mgr_err);

    close(STDIN);
    open(STDIN, "<&" . fileno($work_read));
    #open(STDIN, "<&", $work_read);

    close(STDOUT);
    open(STDOUT, ">&" . fileno($work_write));
    STDOUT->autoflush(1);

    close(STDERR);
    open(STDERR, ">&" . fileno($work_err));
    STDERR->autoflush(1);

    return 0, {
        'pid'        => $$,
        'work_read'  => $work_read,
        'work_write' => $work_write,
        'work_err'   => $work_err,
    };
};

sub new {
    my $class = shift || __PACKAGE__;
    my $args  = shift || {};
    my $self  = {
        max_count => $args->{max_count} || 5,

        rfds      => "",
        count     => 0,
        busy      => 0,
        procs     => {},
    };
    return bless $self, $class;
} # new

sub start {
    my $self     = shift;
    my $callback = shift;
    my $params   = shift;

    if ($self->{count} < $self->{max_count}) {
        my ($pid, $proc) = $fork->();
        return undef if not defined($pid);

        $self->{count} += 1;
        $proc->{job} = $self->{count};

        if ($pid > 0) {
            # parent
            $proc->{parent}   = 1;
            $proc->{callback} = $callback;
            $proc->{params}   = $params;
            $proc->{busy}     = 1;

            $self->{busy} += 1;

            vec($self->{rfds}, fileno($proc->{mgr_read}), 1) = 1;

            $self->{procs}{$proc->{job}} = $proc;
            return $proc;
        }

        # child
        $proc->{child} = 1;
        $renew->($self);
        return $proc;
    }

    foreach my $proc (values(@{$self->{procs}})) {
        next if ($proc->{busy});
        $proc->{callback} = $callback;
        $proc->{params}   = $params;
        $proc->{busy}     = 1;

        $self->{busy} += 1;

        return $proc;
    } # foreach

    return undef;
} # start

sub check_done {
    my $self = shift;
    my $rfds = $self->{rfds};

    return if ($rfds eq q{});

    my $nfound = select($rfds, undef, undef, 0);
    return if ($nfound == 0);

    foreach my $proc (values(%{$self->{procs}})) {
        my $ready = vec($self->{rfds}, fileno($proc->{mgr_read}), 1);
        if ($ready == 1) {
            my $callback = $proc->{callback};
            my $params   = $proc->{params};

            $proc->{busy} = $callback->($proc, $params);
            if (not $proc->{busy}) {
                $self->{busy} -= 1;
            }
        }
    } # foreach
} # check_done

sub is_busy {
    my $self = shift;
    return $self->{busy} > 0;
} # is_busy

1;

__END__
