use inc::Module::Install;

name 'bdshell';
license 'perl';
all_from 'script/bdshell';

requires 'autodie';
requires 'IO::File';
requires 'Getopt::Long';
requires 'Pod::Usage';
requires 'DBD::Pg';
requires 'JSON';
requires 'Text::CSV';

recommends 'Text::CSV_XS';
recommends 'JSON::XS';

install_script 'bdshell';

WriteAll;

1;
