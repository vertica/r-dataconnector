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
#include <fstream>
#include <vector>
#include <folly/Benchmark.h>
#include <folly/Foreach.h>
#include <glog/logging.h>
#include "text/csv/rows.hpp"

#include "base/scopedfile.h"
#include "base/utils.h"
#include "ddc/ddc.h"
#include "assembler/assemblerfactory.h"
#include "blockreader/blockreaderfactory.h"
#include "recordparser/recordparserfactory.h"
#include "splitproducer/splitproducerfactory.h"

namespace ddc{
namespace recordparser{
namespace benchmark{


void helper(const bool dropCaches, const uint64_t blockSize) {
    if(dropCaches) {
        system ("sync");
        system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    }


    std::string filename = "../ddc/test/data/test512MB.csv";
    base::ScopedFile file(filename);
    blockreader::IBlockReaderPtr b = blockreader::BlockReaderFactory::makeBlockReader(base::utils::getExtension(filename));
    splitproducer::ISplitProducerPtr s = splitproducer::SplitProducerFactory::makeSplitProducer(base::utils::getExtension(filename));
    IRecordParserPtr r = RecordParserFactory::makeRecordParser(base::utils::getExtension(filename));

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

    base::ConfigurationMap recordParserConf;
    recordParserConf["splitProducer"] = s;
    std::string schema = "000:int32_t,"
                  "001:int32_t,002:int32_t,003:int32_t,004:int32_t,005:int32_t,006:int32_t,007:int32_t,008:int32_t,"
                  "009:int32_t,010:int32_t,011:int32_t,012:int32_t,013:int32_t,014:int32_t,015:int32_t,016:int32_t,"
                  "017:int32_t,018:int32_t,019:int32_t,020:int32_t,021:int32_t,022:int32_t,023:int32_t,024:int32_t,"
                  "025:int32_t,026:int32_t,027:int32_t,028:int32_t,029:int32_t,030:int32_t,031:int32_t,032:int32_t,"
                  "033:int32_t,034:int32_t,035:int32_t,036:int32_t,037:int32_t,038:int32_t,039:int32_t,040:int32_t,"
                  "041:int32_t,042:int32_t,043:int32_t,044:int32_t,045:int32_t,046:int32_t,047:int32_t,048:int32_t,"
                  "049:int32_t,050:int32_t,051:int32_t,052:int32_t,053:int32_t,054:int32_t,055:int32_t,056:int32_t,"
                  "057:int32_t,058:int32_t,059:int32_t,060:int32_t,061:int32_t,062:int32_t,063:int32_t,064:int32_t,"
                  "065:int32_t,066:int32_t,067:int32_t,068:int32_t,069:int32_t,070:int32_t,071:int32_t,072:int32_t,"
                  "073:int32_t,074:int32_t,075:int32_t,076:int32_t,077:int32_t,078:int32_t,079:int32_t,080:int32_t,"
                  "081:int32_t,082:int32_t,083:int32_t,084:int32_t,085:int32_t,086:int32_t,087:int32_t,088:int32_t,"
                  "089:int32_t,090:int32_t,091:int32_t,092:int32_t,093:int32_t,094:int32_t,095:int32_t,096:int32_t,"
                  "097:int32_t,098:int32_t,099:int32_t,100:int32_t,101:int32_t,102:int32_t,103:int32_t,104:int32_t,"
                  "105:int32_t,106:int32_t,107:int32_t,108:int32_t,109:int32_t,110:int32_t,111:int32_t,112:int32_t,"
                  "113:int32_t,114:int32_t,115:int32_t,116:int32_t,117:int32_t,118:int32_t,119:int32_t,120:int32_t,"
                  "121:int32_t,122:int32_t,123:int32_t,124:int32_t,125:int32_t,126:int32_t,127:int32_t";
    recordParserConf["schema"] = parseSchema(schema);
    r->configure(recordParserConf);

    assembler::IAssemblerPtr fakeAssembler = assembler::AssemblerFactory::makeAssembler("fake");
    r->registerListener(fakeAssembler.get());

    std::vector<boost::any> records;
    while(r->hasNext()) {
        boost::any record = r->next();
        records.push_back(record);
    }

    LOG(INFO) << records.size();
}

BENCHMARK(CsvFromDisk1MB) {
    helper(true, (uint64_t)(1 * 1024 * 1024));
}
BENCHMARK(CsvFromMemory1MB) {
    helper(false, (uint64_t)(1 * 1024 * 1024));
}
BENCHMARK(CsvFromDisk32MB) {
    helper(true, (uint64_t)(32 * 1024 * 1024));
}
BENCHMARK(CsvFromMemory32MB) {
    helper(false, (uint64_t)(32 * 1024 * 1024));
}

void helper2(bool dropCaches) {
    if(dropCaches) {
        system ("sync");
        system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
    }
    std::ifstream infile("../ddc/test/data/test512MB.csv");
    uint64_t nLines = 0;
    std::string line;
    std::vector<std::string> records;

    while (infile >> line)
    {
        std::istringstream ss(line);
        text::csv::csv_istream is(ss);
        text::csv::row row;
        is >> row;
        for(int i = 0; i < row.size(); i++) {
            records.push_back(row[i]);
        }
        ++nLines;
    }
    LOG(INFO) << "nlines: " << nLines;
    LOG(INFO) << "nrecords: " << records.size();
}

BENCHMARK(RawCsvFromDisk) {
    helper2(true);
}
BENCHMARK(RawCsvFromMemory) {
    helper2(false);
}

} // namespace benchmark
} // namespace recordparser
} // namespace ddc

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    folly::runBenchmarks();
}
