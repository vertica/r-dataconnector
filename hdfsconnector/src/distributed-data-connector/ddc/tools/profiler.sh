#!/bin/bash
CPUPROFILE=/tmp/$1.prof LD_PRELOAD=/usr/local/lib/libprofiler.so:/usr/local/lib/libtcmalloc.so GLOG_logtostderr=1 GLOG_minloglevel=0 ./$1
pprof --gv assembler_benchmark /tmp/$1.prof
