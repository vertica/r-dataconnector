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


#include <stdlib.h>
#include <vector>
#include <folly/Benchmark.h>
#include <folly/Foreach.h>
#include <glog/logging.h>

#include "base/scopedfile.h"
#include "blockreader/blockreaderfactory.h"

namespace ddc{
namespace blockreader{
namespace benchmark{

using namespace std;
using namespace folly;

void helper(const uint64_t blockSize) {
    system ("sync");
    system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    const std::string filename = "../ddc/test/data/test512MB.csv";
    IBlockReaderPtr b = BlockReaderFactory::makeBlockReader(filename);
    base::ConfigurationMap conf;
    conf["blocksize"] = blockSize;
    conf["filename"] = filename;
    b->configure(conf);

    base::ScopedFile file(filename);
    base::FileStatus st = file.stat();
    blockreader::BlockPtr block = b->getBlock(0, st.length);
    LOG(INFO) << block->used;
}

BENCHMARK(Local1MB) {
    helper(1 * 1024 * 1024);
}
BENCHMARK(Local32MB) {
    helper(32 * 1024 * 1024);
}
BENCHMARK(dd4K) {
    system ("sync");
    system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    system ("dd if=../ddc/test/data/test512MB.csv of=/dev/null bs=4k");
}
BENCHMARK(dd1MB) {
    system ("sync");
    system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    system ("dd if=../ddc/test/data/test512MB.csv of=/dev/null bs=1M");
}
//BENCHMARK(Hdfs) {
//}

} // namespace benchmark
} // namespace blockreader
} // namespace ddc

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
  folly::runBenchmarks();
}
