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
#include <Rcpp.h>
#include <RInside.h>

#include "base/ifile.h"
#include "base/utils.h"
#include "ddc/ddc.h"
#include "hdfsutils/filefactory.h"

namespace ddc{
namespace recordparser{
namespace benchmark{


void helper(const std::string& url,
            const bool dropCaches,
            const std::string& objType) {
    if(dropCaches) {
        LOG(INFO) << "Dropping caches ...";
        int res, res2;
        if ((res = system ("sync")) == -1) {
            throw std::runtime_error("Error running sync");
        }
        if ((res2 = system ("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")) == -1) {
            throw std::runtime_error("Error dropping caches");
        }
    }

    //std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    std::string filename = base::utils::stripProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename);
    if( hdfsutils::isHdfs(protocol)) {
        hdfsutils::HdfsFile *p = (hdfsutils::HdfsFile *)file.get();
        base::ConfigurationMap hdfsconf;
        hdfsconf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
        p->configure(hdfsconf);
    }
    base::FileStatus status = file->stat();


    base::ConfigurationMap conf;

    std::string schema = "000:int64,"
                  "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
                  "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64,016:int64,"
                  "017:int64,018:int64,019:int64,020:int64,021:int64,022:int64,023:int64,024:int64,"
                  "025:int64,026:int64,027:int64,028:int64,029:int64,030:int64,031:int64,032:int64,"
                  "033:int64,034:int64,035:int64,036:int64,037:int64,038:int64,039:int64,040:int64,"
                  "041:int64,042:int64,043:int64,044:int64,045:int64,046:int64,047:int64,048:int64,"
                  "049:int64,050:int64,051:int64,052:int64,053:int64,054:int64,055:int64,056:int64,"
                  "057:int64,058:int64,059:int64,060:int64,061:int64,062:int64,063:int64,064:int64,"
                  "065:int64,066:int64,067:int64,068:int64,069:int64,070:int64,071:int64,072:int64,"
                  "073:int64,074:int64,075:int64,076:int64,077:int64,078:int64,079:int64,080:int64,"
                  "081:int64,082:int64,083:int64,084:int64,085:int64,086:int64,087:int64,088:int64,"
                  "089:int64,090:int64,091:int64,092:int64,093:int64,094:int64,095:int64,096:int64,"
                  "097:int64,098:int64,099:int64,100:int64,101:int64,102:int64,103:int64,104:int64,"
                  "105:int64,106:int64,107:int64,108:int64,109:int64,110:int64,111:int64,112:int64,"
                  "113:int64,114:int64,115:int64,116:int64,117:int64,118:int64,119:int64,120:int64,"
                  "121:int64,122:int64,123:int64,124:int64,125:int64,126:int64,127:int64";

    conf["schemaUrl"] = schema;
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;

    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf")   ;

    boost::shared_ptr<Rcpp::DataFrame> dfptr =
        boost::any_cast<boost::shared_ptr<Rcpp::DataFrame>>(ddc_read(url,
                                                                     objType,
                                                                     conf));
        Rcpp::DataFrame df = Rcpp::DataFrame(*(dfptr.get()));

    LOG(INFO) << "Got dataframe of size " << df.size();
}


BENCHMARK(CsvFromDisk) {
    helper("../ddc/test/data/test512MB.csv", true, "rdataframe");
}
BENCHMARK(CsvFromHdfs) {
    helper("hdfs:///test512MB.csv", false, "rdataframe");
}

} // namespace benchmark
} // namespace recordparser
} // namespace ddc

int main(int argc, char *argv[]) {
    RInside R(argc, argv);
    google::InitGoogleLogging(argv[0]);
    folly::runBenchmarks();
}
