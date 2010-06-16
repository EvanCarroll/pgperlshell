#!/usr/bin/env perl
use Carp;
use strict;
use warnings;
use autodie;
use feature ':5.10';

use XXX;
use Data::Dumper;
use DBD::Pg;
use Text::CSV;
use IO::File;
use Getopt::Long;
use Pod::Usage;

my ($un, $db, $pw, $help);
my $result = GetOptions(
	'username|U=s' => \$un
	, 'dbname|d=s' => \$db
	, 'password|P=s' => \$pw
	, 'help|?' => \$help
);
pod2usage(-verbose => 2) if $help;

my $dbh = DBI->connect("dbi:Pg:dbname=$db", $un, $pw, {RaiseError=>1})
	or die DBI::errstr;
die 'not connected' unless $dbh->ping == 1;

my $copy = do {
	use Regexp::Grammars;
	qr{
		<action=(\\COPY)> <from>

		<rule: from>            <table=literal> <columns=pcolumns>? FROM <source> <with>?

		<rule: with>            WITH? (<[MATCH=with_options]> ** \s)
		<token: with_options>   binary|oids|<delimiter>|<null>|<header>

		<rule: delimiter>       DELIMITER <.as>? <MATCH=pair>
		<rule: null>            NULL <.as>? <MATCH=pair>

		<rule: header>          CSV ( <[MATCH=header_options]>  ** \s )
		<token: header_options> header|<quote>|<escape>|<force_not_null>|<custom_tcsv>|<custom_pl>
		<rule: quote>           QUOTE <.as>? <MATCH=pair>
		<rule: escape>          ESCAPE <.as>? <MATCH=pair>
		<rule: force_not_null>  FORCE NOT NULL <MATCH=columns>

		<rule: custom_tcsv>     -TCSV_<key=(\w+)> = <value=literal>
		<rule: custom_pl>       -PERL_<key=(\w+)> = <value=literal>

		<token: source>         stdin|<MATCH=literal>
		<token: as>             AS

		<rule: columns>         (<[MATCH=literal]> ** \,)+
		<rule: pcolumns>        \( <MATCH=columns> \)

		<rule: pair>            <delim=(\$\$|"|')> <MATCH=literal> (??{ quotemeta $MATCH{delim} })
		<token: literal>        \S+
	}xmsi;
};


sub process_copy {
	my $copy = shift;

	my $tcsv_args = {binary=>1,blank_is_undef=>1};
	my $perl_args;

	croak 'Invalid SQL' unless $copy->{action} =~ /COPY/;

	## SQL processing
	foreach my $opt ( @{$copy->{from}{with}} ) {
		given ( $opt ) {
			when ( qr/binary/i ) {
				die 'not handeled'
			}
			when ( qr/oids/i ) {
				die 'not handeled'
			}
			when ( qr/delimiter/i ) {
				$tcsv_args->{sep_char} = $opt->{delimiter};
			}
			when ( qr/null/i    ) {
				$tcsv_args->{empty_is_undef} = 1
					if $opt->{null}
				;
			}

			when ( qr/header/i ) {

				foreach my $opt ( @{$opt->{header}} ) {
					given ( $opt ) {
						when ( qr/quote/i   ) { $tcsv_args->{quote_char} = $opt->{quote} }
						when ( qr/escape/i  ) { $tcsv_args->{escape_char} = $opt->{escape} }
						when ( qr/tcsv/i    ) { $_=$opt->{custom_tcsv}; $tcsv_args->{$_->{key}} = $_->{value} }
						when ( qr/header/i  ) { $perl_args->{trash_header} = 1 }
					}
				}

			}

		}
	}

	my $icsv = Text::CSV->new($tcsv_args)
		or die "Cannot use CSV: ".Text::CSV->error_diag()
	;
	my $ocsv = Text::CSV->new({binary=>1});
	
	my $fh  = IO::File->new( $copy->{from}{source}, '<' );

	$fh->getline if $perl_args->{trash_header};
	
	my $new_copy = sprintf (
		q[ COPY %s %s FROM STDIN WITH CSV QUOTE AS '"' ]
		, $copy->{from}{table}
		, @{ $copy->{from}{columns} } ? '(' . join (', ', @{$copy->{from}{columns}}) . ')' : ''
	);

	until ( $icsv->eof ) {
		die $icsv->error_diag if $icsv->error_diag && ! $icsv->eof;
		my $tuple = $icsv->getline($fh);
		$ocsv->combine( @$tuple );
		say $ocsv->string;
	}



}

my $sql;
$sql = q[ \COPY table ( foo, bar, baz ) FROM cobalt.csv WITH CSV HEADER QUOTE '"' -TCSV_allow_loose_quotes = 1 ];
$sql =~ $copy;
process_copy ( \%/ );

__END__

=head1 NAME

BirghtDog Shell - a shell for Postgresql

=head2 COPY

More information can be found here: http://www.postgresql.org/docs/current/interactive/sql-copy.html

	COPY tablename [ ( column [, ...] ) ]
		FROM { 'filename' | STDIN }
		[ [ WITH ]
				[ BINARY ]
				[ OIDS ]
				[ DELIMITER [ AS ] 'delimiter' ]
				[ NULL [ AS ] 'null string' ]
				[ CSV [ HEADER ]
					[ QUOTE [ AS ] 'quote' ]
					[ ESCAPE [ AS ] 'escape' ]
					[ FORCE NOT NULL column [, ...] ]


	COPY { tablename [ ( column [, ...] ) ] | ( query ) }
		TO { 'filename' | STDOUT }
		[ [ WITH ]
			[ BINARY ]
			[ OIDS ]
			[ DELIMITER [ AS ] 'delimiter' ]
			[ NULL [ AS ] 'null string' ]
			[ CSV [ HEADER ]
				[ QUOTE [ AS ] 'quote' ]
				[ ESCAPE [ AS ] 'escape' ]
				[ FORCE QUOTE column [, ...] ]
