#!/bin/bash
echo 'Run this from Debug/Release dir'
python ../tools/create_csv_files.py 4 16 15 ../ddc/test/data/test4MB.csv 32
python ../tools/create_csv_files.py 4 16 15 ../ddc/test/data/test4MB.offsetcsv 32
python ../tools/create_csv_files.py 512 128 15 ../ddc/test/data/test512MB.csv 32
python ../tools/create_csv_files.py 512 128 15 ../ddc/test/data/test512MB.offsetcsv 32
python ../tools/create_csv_files.py 512 10 15 ../ddc/test/data/test512MB_10col.csv 32
echo "000000033554432,000000033554433,000000033554434,000000033554435,000000033554436,000000033554437,000000033554438,000000033554439" >> ../ddc/test/data/test512MB_10col.csv
