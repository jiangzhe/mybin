#!/bin/bash -eu

if [ $# -ne 2 ]; then
    echo "Usage: $0 name port"
    exit 4
fi

CURR_SCRIPT=$(readlink -f $0)
CURR_DIR=$(dirname $CURR_SCRIPT)

docker run -d --rm --name "$1"  -v $CURR_DIR/mysqld-5.5.cnf:/etc/mysql/my.cnf -p $2:3306 -e MYSQL_ROOT_PASSWORD=password mysql:5.5.50
