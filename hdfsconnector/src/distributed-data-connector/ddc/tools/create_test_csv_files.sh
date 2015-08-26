#!/bin/bash
echo 'Run this from Debug/Release dir'
python ../tools/create_csv_files.py 4 16 15 ../ddc/test/data/test4MB.csv 32
python ../tools/create_csv_files.py 4 16 15 ../ddc/test/data/test4MB.offsetcsv 32
python ../tools/create_csv_files.py 512 128 15 ../ddc/test/data/test512MB.csv 32
python ../tools/create_csv_files.py 512 128 15 ../ddc/test/data/test512MB.offsetcsv 32
