package bdshell::Backend;
use Moose;
use strict;
use warnings;

use feature ':5.10';

with qw(
	bdshell::Roles::DB
	bdshell::Traits::Csv
);

around 'process_action_copy' => sub {
	my ( $sub, $self, $sql, $input ) = @_;
	
	state $dbh = $self->_dbh;

	$dbh->do(
		sprintf (
			q[ COPY %s %s FROM STDIN WITH CSV QUOTE AS '"' ]
			, $sql->{table}
			, exists $sql->{columns} ? '(' . join (', ', @{$sql->{columns}}) . ')' : ''
		)
	);

	$self->$sub( $sql, $input );
	
	$dbh->pg_putcopyend;
};

sub process_action_set {
	my ( $self, $sql, $input ) = @_;
	s/;.*// for $sql->{key}, $sql->{value};
	$self->db_set( $sql->{key}, $sql->{value} );
}

sub process_action_echo {
	my ( $self, $sql, $input ) = @_;
	say $sql->{string};	
}


sub _process_default {
	my ( $self, $sql, $orig_input ) = @_;
	my $dbh = $self->_dbh;
	say "FALL THROUGH:\n\t$orig_input" if $bdshell::DEBUG;
	$dbh->do( $orig_input );
}

__PACKAGE__->meta->make_immutable;
