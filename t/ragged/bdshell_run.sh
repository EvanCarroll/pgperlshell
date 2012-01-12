bdshell -d dealermade -c 'TRUNCATE TABLE bdshell_test.ragged;'

## Should die no idea how many columns the table is, can't trucate data
bdshell -d dealermade -c '\COPY bdshell_test.ragged FROM data/ten_column_load_normal.csv WITH -PERL_ragged'

## Should insert 1,2,3,4
bdshell -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_normal.csv WITH -PERL_ragged'

## Should insert 1,2,3,4
bdshell -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_backwards.csv WITH -PERL_input_order "\10 \9 \8 \7"'

## Should die no idea on how to handle spaces.
bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b , c,d) FROM data/ten_column_load_normal_with_spaces.csv WITH -PERL_ragged'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (d,c,b,a) FROM data/ten_column_load_normal_header.csv WITH -PERL_header -PERL_input_order="d, c, b, a"'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_normal_header.csv WITH -PERL_header -PERL_input_order="a, b, c, d"'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_backwards_header.csv WITH -PERL_header -PERL_input_order="a, b, c, d"'


