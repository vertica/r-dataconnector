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
