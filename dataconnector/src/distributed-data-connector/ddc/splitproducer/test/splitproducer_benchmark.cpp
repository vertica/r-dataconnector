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
#include "base/utils.h"
#include "blockreader/blockreaderfactory.h"
#include "splitproducer/splitproducerfactory.h"

namespace ddc{
namespace splitproducer{
namespace benchmark{

using namespace std;
using namespace folly;

void helper(const bool dropCaches, const uint64_t blockSize) {
    if(dropCaches) {
        system ("sync");
        system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    }


    std::string filename = "../ddc/test/data/test512MB.csv";
    base::ScopedFile file(filename);
    blockreader::IBlockReaderPtr b = blockreader::BlockReaderFactory::makeBlockReader(base::utils::getExtension(filename));
    ISplitProducerPtr s = SplitProducerFactory::makeSplitProducer(base::utils::getExtension(filename));

    base::ConfigurationMap blockReaderConf;
    blockReaderConf["blocksize"] = blockSize;
    blockReaderConf["filename"] = filename;
    b->configure(blockReaderConf);

    base::ConfigurationMap splitProducerConf;
    splitProducerConf["delimiter"] = (uint8_t)'\n';
    splitProducerConf["splitStart"] = (uint64_t)0;
    splitProducerConf["splitEnd"] = file.stat().length;
    splitProducerConf["fileEnd"] = file.stat().length;
    splitProducerConf["blockReader"] = b;
    s->configure(splitProducerConf);

    std::vector<boost::shared_ptr<Split> > splits;
    while(s->hasNext()) {
          splits.push_back(s->next());
    }
    LOG(INFO) << splits.size();
}

BENCHMARK(DelimiterFromDisk1MB) {
    helper(true, (uint64_t)(1 * 1024 * 1024));
}
BENCHMARK(DelimiterFromMemory1MB) {
    helper(false, (uint64_t)(1 * 1024 * 1024));
}
BENCHMARK(DelimiterFromDisk32MB) {
    helper(true, (uint64_t)(32 * 1024 * 1024));
}
BENCHMARK(DelimiterFromMemory32MB) {
    helper(false, (uint64_t)(32 * 1024 * 1024));
}

} // namespace benchmark
} // namespace splitproducer
} // namespace ddc

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    folly::runBenchmarks();
}
