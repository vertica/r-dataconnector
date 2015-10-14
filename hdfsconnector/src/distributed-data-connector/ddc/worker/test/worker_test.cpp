/*
(c) Copyright 2015 Hewlett Packard Enterprise Development LP

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


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
