#!/bin/bash

while true; do

    R -e "library(HPdata);distributedR_start(inst=64); df <- csv2dframe(url='hdfs:///user/hive/warehouse/int_10_columns_1b_rows_txt/int_10_columns_1b_rows.part-*' , schema='c1:int64,c2:int64,c3:int64,c4:int64,c5:int64,c6:int64,c7:int64,c8:int64,c9:int64,c10:int64', delimiter='|')" 2>&1|tee log
    if [ $? -ne 0 ]; then
        break
    fi
done
