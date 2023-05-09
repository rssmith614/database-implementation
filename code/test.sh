#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: test.sh [query_number_start] [query_number_end]"
    exit 1
fi

if [ $# -eq 2 ]; then
    START=$1
    END=$2
    RES=0
    > output_diff.txt

    for (( I=$START; I<=$END; I++))
    do
        sql="../queries/demo/$I.sql" 
        > output.txt
        echo $sql >> output_diff.txt

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

        echo "==========================================================" >> output_diff.txt
    done
    
    let "N = $END - $START + 1"

    echo passed $RES / $N queries
else
    sql="../queries/demo/$1.sql"
    > output.txt

    sqlite3 ../data/tpch.sqlite < $sql > sqlite_output.txt
    execs/test-query.out < $sql

    sort -o output.txt{,}
    sort -o sqlite_output.txt{,}

    if ! numdiff output.txt sqlite_output.txt > output_diff.txt; then
        echo sqlite output was different
        exit 1
    fi
fi