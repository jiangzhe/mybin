#!/bin/bash -eu

CURR_SCRIPT=$(readlink -f $0)
CURR_DIR=$(dirname $CURR_SCRIPT)

if [ $# -ne 2 ]; then
    echo "Usage: $0 name port"
    exit 4
fi

MYSQL_NAME=$1
MYSQL_PORT=$2

docker run -d --rm --name "$MYSQL_NAME" -v $CURR_DIR/mysqld.cnf:/etc/mysql/mysql.conf.d/mysqld.cnf -v $CURR_DIR:/mnt/data -p $MYSQL_PORT:3306 -e MYSQL_ROOT_PASSWORD=password mysql:5.7.30

conn_retries=0
while [ true ]; do
    set +e
    nc -z localhost $MYSQL_PORT
    rcode=$?
    if [ $rcode -ne 0 ]; then
        (( conn_retries = conn_retries + 1 ))
        if [ $conn_retries -lt 20 ]; then 
            sleep 1
        else
            echo "failed to start mysql docker with limited time, check local port setting"
            exit 4
        fi
    else
        break
    fi
done

