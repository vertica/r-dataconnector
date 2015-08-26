#!/bin/bash

BUILD_TYPE=Release

cd ../$BUILD_TYPE; make -j`nproc` ddc_test ddcmaster_test; cd ../tools;
../$BUILD_TYPE/ddc_test
if [ $? -ne 0 ]; then
    echo "Test failed"
    exit 1
fi
../$BUILD_TYPE/ddcmaster_test
if [ $? -ne 0 ]; then
    echo "Test failed"
    exit 1
fi

exit 0
