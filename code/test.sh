#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: test.sh [query_number]"
    exit 1
fi

if [ $1 = "all" ]; then
    N=20
    RES=0
    for I in {1..20}
    do
        sql="../queries/phase-4/$I.sql"

        execs/test-query.out < $sql > /dev/null
        sqlite3 ../data/tpch.sqlite | sort < $sql > sqlite_output.txt

        if numdiff <(sort output.txt) sqlite_output.txt > /dev/null; then
            echo passed $I.sql
            ((RES=RES+1))
        else
            echo failed $I.sql
        fi
    done
    
    echo passed $RES / $N queries
else
    sql="../queries/phase-4/$1.sql"

    execs/test-query.out < $sql > /dev/null
    sqlite3 ../data/tpch.sqlite | sort < $sql > sqlite_output.txt

    if ! numdiff output.txt sqlite_output.txt > output_diff.txt; then
        echo sqlite output was different
        exit 1
    fi
fi