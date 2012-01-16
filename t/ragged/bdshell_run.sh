bdshell -d dealermade -c 'TRUNCATE TABLE bdshell_test.ragged;'

## Should die no idea how many columns the table is, can't trucate data
echo "This should error:";
bdshell -d dealermade -c '\COPY bdshell_test.ragged FROM data/ten_column_load_normal.csv WITH FORMAT csv -PERL_ragged'

## This file has two values, "1,3" static inserts responsible for 2 and 4
bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/two_column_load_normal.csv WITH FORMAT csv -PERL_static_column=$${"b": 2, "d": 4}$$;'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_normal.csv WITH FORMAT csv -PERL_ragged -PERL_static_column=$${"d": 4}$$;'

echo "This should error:";
bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_normal.csv WITH FORMAT csv -PERL_static_column=$${"d": 4}$$;'

## All of these input files should insert 1,2,3,4
bdshell -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_normal.csv WITH FORMAT csv -PERL_ragged'

bdshell -d dealermade -c '\COPY bdshell_test.ragged ( a, b, c, d ) FROM data/ten_column_load_backwards.csv WITH FORMAT csv -PERL_input_order "\10 \9 \8 \7"'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b , c,d) FROM data/ten_column_load_normal_with_spaces.csv WITH FORMAT csv -PERL_ragged'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (d,c,b,a) FROM data/ten_column_load_normal_header.csv WITH FORMAT csv -PERL_header -PERL_input_order="d, c, b, a"'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_normal_header.csv WITH FORMAT csv -PERL_header -PERL_input_order="a, b, c, d"'

bdshell -d dealermade -c '\COPY bdshell_test.ragged (a,b,c,d) FROM data/ten_column_load_backwards_header.csv WITH FORMAT csv -PERL_header -PERL_input_order="a, b, c, d"'
