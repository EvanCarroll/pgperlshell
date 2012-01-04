use inc::Module::Install;

name 'bdshell';
license 'perl';
all_from 'script/bdshell';

requires 'autodie';
requires 'IO::File';
requires 'Getopt::Long';
requires 'Pod::Usage';
requires 'DBD::Pg';

install_script 'bdshell';

WriteAll;

1;
