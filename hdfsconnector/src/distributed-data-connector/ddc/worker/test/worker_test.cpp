#include <boost/thread.hpp>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "worker/worker.h"

#include <RInside.h>  // needs to be included the last one, otherwise compile errors

int main(int argc, char *argv[]) {
    RInside R(argc, argv); //TODO error-prone, segfaults if this line is missing
    google::InitGoogleLogging(argv[0]);
    using namespace ddc::distributor;
    ddc::distributor::Worker w;
    w.start();
    w.join();
}
