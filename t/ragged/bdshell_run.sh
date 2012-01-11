psql -d dealermade -c 'truncate table bdshell_test.ragged';

## Should die no idea how many columns the table is, can't trucate data
bdshell -d dealermade -c '\COPY bdshell_test.ragged FROM data/ten_column_loadnormal.txt WITH -PERL_ragged'

## Should insert 1,2,3,4
bdshell -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_normal.txt WITH -PERL_ragged'

## Should insert 1,2,3,4
bdshell --debug -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_backwards.txt WITH -PERL_input_order "\10 \9 \8 \7"'

## Should die no idea on how to handle spaces.
bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b , c,d) FROM data/ten_column_load_normal_with_spaces.txt WITH -PERL_ragged'

