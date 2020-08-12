#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 filename"
    exit 4
fi

FILE=$(readlink -f $1)

docker run --rm --entrypoint='mysqlbinlog' -v $FILE:/mnt/binlog mysql:5.7.30 --base64-output=decode-rows --verbose /mnt/binlog

