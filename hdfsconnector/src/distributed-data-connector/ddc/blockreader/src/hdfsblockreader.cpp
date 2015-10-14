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


#include "hdfsblockreader.h"

namespace ddc{
namespace blockreader {

HdfsBlockReader::HdfsBlockReader() :
    blockSize_(0),
    configured_(false),
    fileSize_(0)
{
}

HdfsBlockReader::~HdfsBlockReader()
{

}

void HdfsBlockReader::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(filename_, std::string, "filename");
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");
    GET_PARAMETER(fileStatCache_, boost::shared_ptr<base::Cache>, "fileStatCache");

    hdfsutils::HdfsFile file(filename_);
    base::ConfigurationMap blockLocatorConf;
    blockLocatorConf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    blockLocatorConf["fileStatCache"] = fileStatCache_;
    file.configure(blockLocatorConf);
    base::FileStatus s = file.stat();
    blockSize_ = s.blockSize;
    fileSize_ = s.length;
    configured_ = true;
}

BlockPtr HdfsBlockReader::next()
{
    throw std::runtime_error("unimplemented");
}


bool HdfsBlockReader::hasNext()
{
    throw std::runtime_error("unimplemented");

}

BlockPtr HdfsBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes)
{
    if(!configured_) {
        throw std::runtime_error("need to configure");
    }

    if(numBytes == 0) {
        throw std::runtime_error("requested 0 numBytes");
    }

    if (blockStart >= fileSize_) {
        // return empty buffer
        boost::shared_ptr<std::vector<uint8_t> > v = boost::shared_ptr<std::vector<uint8_t> >(new std::vector<u_int8_t>);
        BlockPtr block = BlockPtr(new Block(v));
        return block;
    }

    hdfsutils::HdfsBlockLocator locator;
    base::ConfigurationMap conf;
    conf["filename"] = filename_;
    conf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    conf["fileStatCache"] = fileStatCache_;
    locator.configure(conf);
    hdfsutils::BufferPtr buffer = locator.getBlock(blockStart, numBytes);

    BlockPtr block = BlockPtr(new Block(buffer));
    return block;
}

} // namespace blockreader
} // namespace ddc

