#!/bin/bash
echo -e "catch exception unhandled\\n\
file $1\\n\
run --gtest_filter=$2 --gtest_catch_exceptions=0\\n\
bt" > __gdb.txt
cat __gdb.txt | gdb
rm __gdb.txt
