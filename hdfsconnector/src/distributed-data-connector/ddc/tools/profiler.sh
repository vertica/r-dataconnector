#!/bin/bash
# Usage: binary, args, name
CPUPROFILE=/tmp/$3.prof LD_PRELOAD=/usr/local/lib/libprofiler.so:/usr/lib/libtcmalloc.so GLOG_logtostderr=1 GLOG_minloglevel=0 "$1" "$2"
pprof --gv "$1" /tmp/$3.prof
