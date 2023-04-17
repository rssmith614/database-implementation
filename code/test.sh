#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: test.sh [query_number]"
    exit 1
fi

if [ $1 = "all" ]; then
    N=20
    RES=0
    > output_diff.txt
    for I in {1..20}
    do
        sql="../queries/phase-4/$I.sql" 
        echo $sql >> output_diff.txt $'\n'

        sqlite3 ../data/tpch.sqlite < $sql > sqlite_output.txt
        execs/test-query.out < $sql > /dev/null

        sort -o output.txt{,}
        sort -o sqlite_output.txt{,}

        if numdiff output.txt sqlite_output.txt >> output_diff.txt; then
            echo passed $I.sql
            ((RES=RES+1))
        else
            echo failed $I.sql
        fi
    done
    
    echo passed $RES / $N queries
else
    sql="../queries/phase-4/$1.sql"

    sqlite3 ../data/tpch.sqlite < $sql > sqlite_output.txt
    execs/test-query.out < $sql > /dev/null

    sort -o output.txt{,}
    sort -o sqlite_output.txt{,}

    if ! numdiff output.txt sqlite_output.txt > output_diff.txt; then
        echo sqlite output was different
        exit 1
    fi
fi