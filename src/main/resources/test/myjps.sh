#!/bin/bash

for host in wbbigdata00 wbbigdata01 wbbigdata02; do
    echo ===================$host==================
    ssh $host "source /etc/profile; jps"
done
