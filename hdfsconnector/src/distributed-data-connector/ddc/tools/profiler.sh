#!/bin/bash
# Usage: binary, args, name
PROFILENAME=$1
BINARY=$2
shift 
shift
CPUPROFILE=/tmp/$PROFILENAME.prof LD_PRELOAD=/usr/local/lib/libprofiler.so:/usr/lib/libtcmalloc.so GLOG_logtostderr=1 GLOG_minloglevel=0 $BINARY $*
pprof --gv $BINARY /tmp/$PROFILENAME.prof
