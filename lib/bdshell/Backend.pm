package bdshell::Backend;
use Moose;
use strict;
use warnings;

use feature ':5.10';

with 'bdshell::Roles::DB';

sub process_action_set {
	my ( $self, $sql, $input ) = @_;
	s/;.*// for $sql->{key}, $sql->{value};
	$self->db_set( $sql->{key}, $sql->{value} );
}

sub process_action_echo {
	my ( $self, $sql, $input ) = @_;
	say $sql->{string};	
}

sub process_action_copy {
	my ( $self, $sql, $input ) = @_;

	my $dbh = $self->_dbh;

	my $format;
	foreach my $opt ( @{$sql->{with}} ) {
		$format = $opt->{format} if $opt ~~ qr/format/i;
	}

	## SQL processing
	my $perl_args;
	my $tcsv_args = { binary => 1, blank_is_undef => 1 };
	foreach my $opt ( @{$sql->{with}} ) {
		next if $opt ~~ qr/format/;
		
		given ( $opt ) {
			when ( qr/binary|oids|force_not_null|force_quote/i ) {
				die "Option $opt not yet supported\n";
				## Force not null would be easily supported
				## but it requires knowing columns names
				## Even if not present in the sql command
			}
			when ( qr/header/i ) {
				$perl_args->{header}=1
			}
			when ( qr/perl/i ) {
				$_=$opt->{custom_perl};
				$perl_args->{$_->{key}} = $_->{value}//1;
			}
		}

		given ( $format ) {
			when ( 'csv' ) {
				given ( $opt ) {
					when ( qr/delimiter/i ) {
						$tcsv_args->{sep_char} = $opt->{delimiter};
					}
					when ( qr/null/i ) {
						$tcsv_args->{empty_is_undef} = 1
							if $opt->{null}
						;
					}
					when ( qr/quote/i ) {
						$tcsv_args->{quote_char} = $opt->{quote};
						# The default escape should be the quote per pg docs
						$tcsv_args->{escape_char} //= $opt->{quote};
					}
					when ( qr/escape/i ) {
						$tcsv_args->{escape_char} = $opt->{escape};
					}
					when ( qr/tcsv/i ) {
						$_=$opt->{custom_tcsv};
						$tcsv_args->{$_->{key}} = $_->{value}//1;
					}
				}
			}
			when ( 'fixedwidth' ) {
				given ( $opt ) {
					when ( qr/fixedwidth/i ) {
						$_=$opt->{custom_fixedwidth};
						$tcsv_args->{$_->{key}} = $_->{value}//1;
					}
				}
			}
			when ( 'text' ) {
				## In this format NULL IS \0
				$opt->{delimiter} = "\t";
			}
		}
			
	}
	
	$dbh->do(
		sprintf (
			q[ COPY %s %s FROM STDIN WITH CSV QUOTE AS '"' ]
			, $sql->{table}
			, exists $sql->{columns} ? '(' . join (', ', @{$sql->{columns}}) . ')' : ''
		)
	);


	my $fh = IO::File->new( $sql->{source}, '<' )
		or die "$! " . $sql->{source} . "\n"
	;

	## [I]nput and [O]utput
	my $icsv = Text::CSV->new($tcsv_args)
		or die "Cannot use CSV: ".Text::CSV->error_diag()
	;
	my $ocsv = Text::CSV->new({binary=>1});

	## Discards the header row, and sets Text::CSV's column_names
	my @header;
	if ( $perl_args->{header} ) {

		@header = $perl_args->{header_lowercase}
			? map lc, @{$icsv->getline($fh)}
			: @{$icsv->getline($fh)}
		;
		
		$icsv->column_names( @header );

		## This code works if you have a header in the CSV file
		## and the column list is specified in the copy-command
		## then, it resolves the names in input_order to
		## numerical values
		my @arr;
		if ( $perl_args->{input_order} !~ /\\/ ) {
			COL: foreach my $col_name ( @{[split ',\s*', $perl_args->{input_order}]} ) {
				for ( my $i = 0; $i <= @header; $i++ ) {
					if ( $header[$i] eq $col_name ) {
						push @arr, $i+1; # the \1 is 1 based.
						next COL;
					}
				}
				die "Failed to find column $col_name\n";
			}
			$perl_args->{input_order} = join ' ', map "\\$_", @arr;
		}

	}
	
	## Skips lines if the option is set
	if ( $perl_args->{skip_lines} ) {
		$fh->getline while $perl_args->{skip_lines}--;
	}
	
	my $static_columns_processor;
	if ( $perl_args->{static_column} ) {

		my $perl_arg = JSON::decode_json( $perl_args->{static_column} );

		my @columns_in_copy = @{$sql->{columns}};
		my $columns_in_copy = {};
		## We need to actually know the positions given in the \COPY
		for ( my $i = 0; $i < @columns_in_copy; $i++ ) {
			$columns_in_copy->{ $columns_in_copy[$i] } = $i;
		}

		## These have to be in order so when we generate
		## splice statements we do ltr
		my @ordered_args;
		while ( my ($k, $v) = each %$perl_arg ) {
			push @ordered_args, [$columns_in_copy->{$k}, $v];
		}
		@ordered_args = sort { $a->[0] <=> $b->[0] } @ordered_args;

		$static_columns_processor = sub {
			my $input = shift;
			foreach my $splice_arg ( @ordered_args ) {
				no warnings;
				splice ( @$input, $splice_arg->[0], 0, $splice_arg->[1] )
			}
		};

	}

	while ( not $fh->eof ) {

		my $tuple = $icsv->getline($fh);

		next unless List::Util::first {defined $_} @$tuple;

		if ( $icsv->error_diag ) {
			my ( $err, $text ) = $icsv->error_diag;
			warn "Text::CSV ErrorCode: [$err], Meaning: [$text]\n";
			$icsv->SetDiag(0);
			next;
		}

		my @columns;

		if ( $perl_args->{input_order} ) {
			my @index = ( $perl_args->{input_order} =~ m/\\?(\d+)\s*/g );
			## -1 because 1 is the 0th element in the csv
			@columns = map $tuple->[$_-1], @index;
		}
		else {
			@columns = @$tuple;
		}

		$static_columns_processor->(\@columns)
			if $static_columns_processor
		;

		## will get around
		## ERROR:  extra data after last expected column
		if ( $perl_args->{ragged} ) {
			if ( $sql->{columns} ) {
				splice ( @columns, @{$sql->{columns}}, $#columns , () );
			}
			else {
				die "Failure to specify columns detected, do not know how many columns I should keep in ragged csv\n";
			}
		}
		
		$ocsv->combine( @columns );
		$dbh->pg_putcopydata( $ocsv->string . "\n" );
	}

	$dbh->pg_putcopyend;

}

sub _process_default {
	my ( $self, $sql, $orig_input ) = @_;
	my $dbh = $self->_dbh;
	say "FALL THROUGH:\n\t$orig_input" if $bdshell::DEBUG;
	$dbh->do( $orig_input );
}

__PACKAGE__->meta->make_immutable;
