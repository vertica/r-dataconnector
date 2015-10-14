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


#include "localblockreader.h"
#include <stdexcept>
#include <glog/logging.h>


namespace ddc{
namespace blockreader {

LocalBlockReader::LocalBlockReader() :
    blockSize_(0),
    filename_(""),
    configured_(false),
    fileSize_(0)
{

}

LocalBlockReader::~LocalBlockReader()
{

}

void LocalBlockReader::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(blockSize_, uint64_t, "blocksize");
    GET_PARAMETER(filename_, std::string, "filename");
    f_ = base::ScopedFilePtr(new base::ScopedFile(filename_));
    fileSize_ = f_->stat().length;
    configured_ = true;
}

BlockPtr LocalBlockReader::next()
{
    throw std::runtime_error("unimplemented");
}


bool LocalBlockReader::hasNext()
{
    throw std::runtime_error("unimplemented");

}

uint64_t LocalBlockReader::blockSize()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return blockSize_;
}

BlockPtr LocalBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes)
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

    //DLOG(INFO) << "blockStart: " << blockStart << " numBytes: " << numBytes;
    f_->seek(blockStart);

    boost::shared_ptr<std::vector<uint8_t> > v = boost::shared_ptr<std::vector<uint8_t> >(new std::vector<u_int8_t>);
    v->resize(numBytes);
    size_t bytesRead = f_->read(v->data(), numBytes);
    if(bytesRead != numBytes) {
        //error
        switch(bytesRead) {
            case 0: {
                if(f_->eof()) throw std::runtime_error("eof");
                else throw std::runtime_error("error reading");
                break;
            }
            default: {
                throw std::runtime_error("read less bytes than expected");
            }
        }
    }

    BlockPtr block = BlockPtr(new Block(v));
    return block;
}

} // namespace blockreader
} // namespace ddc

