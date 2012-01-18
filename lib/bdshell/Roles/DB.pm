package bdshell::Roles::DB;
use strict;
use warnings;
use Moose::Role;

use DBD::Pg;

use feature ':5.10';
use v5.10;

use namespace::autoclean;

has '_dbh' => (
	isa  => 'Object'
	, is => 'ro'
	, lazy => 1
	, default => sub {
		my $self = shift;
		my $db = $self->_db;
		state $dbh = DBI->connect_cached("dbi:Pg:dbname=$db"
			, $self->_un
			, $self->_pw, {RaiseError=>1}
		) or die $DBI::errstr;

		if ( $dbh->ping == 1 ) {
			say 'Connection established' if $bdshell::DEBUG;
		}
		else {
			die 'not connected';
		}

		$dbh;

	}
);

has '_un' => (
	isa => 'Str'
	, is => 'ro'
	, default => $ENV{USER}
);

has '_pw' => (
	isa => 'Maybe[Str]'
	, is => 'ro'
);

has '_db' => (
	isa => 'Str'
	, is => 'ro'
	, default => $ENV{USER}
);

sub db_set {
	my ( $self, $k, $v ) = @_;
	my $dbh = $self->_dbh;
	$dbh->do( sprintf( 'SET %s = %s;', $k, $v ) );
}

1;
