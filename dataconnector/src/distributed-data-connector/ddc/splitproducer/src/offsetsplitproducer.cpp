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


#include "offsetsplitproducer.h"
#include <iostream>
#include <stdexcept>
#include <boost/format.hpp>
#include <glog/logging.h>

namespace ddc {
namespace splitproducer {

OffsetSplitProducer::OffsetSplitProducer()
    : blockSize_(0),
      fileEnd_(0),
      offsetIndex_(0),
      splitsProduced_(0),
      splitsDiscarded_(0),
      configured_(false)
{
}

OffsetSplitProducer::~OffsetSplitProducer()
{

}



void OffsetSplitProducer::configure(base::ConfigurationMap& conf)
{

    GET_PARAMETER(fileEnd_,uint64_t,"fileEnd");
    GET_PARAMETER(offsets_ ,std::vector<uint64_t>,"offsets");
    GET_PARAMETER(blockReader_,blockreader::IBlockReaderPtr,"blockReader");
    offsetIndex_ = 0;
    configured_ = true;

}

base::ConfigurationMap OffsetSplitProducer::getDebugInfo() {
    return base::ConfigurationMap();
}

bool OffsetSplitProducer::hasNext()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return offsetIndex_ < offsets_.size();

}

SplitPtr OffsetSplitProducer::next()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }

    if(offsetIndex_ >= offsets_.size()) {
        throw std::runtime_error("out of file bounds");
    }


    //TODO fileEnd_ is the split end, not the file end
    uint64_t numBytes;
    if(offsetIndex_ < (offsets_.size() - 1)) {
        numBytes = offsets_[offsetIndex_ + 1] - offsets_[offsetIndex_];
    }
    else {
        //last offset
        numBytes = fileEnd_ - offsets_[offsetIndex_];
    }
    block_ = blockReader_->getBlock(offsets_[offsetIndex_], numBytes);

    //DLOG(INFO) << "requesting block, offset: " << offsets_[offsetIndex_] << " numBytes: " << numBytes;
    DLOG_IF(INFO, (splitsProduced_ < 10)) <<  boost::format(" Completed %3.2f%%\r")  % (100 * (float)offsets_[offsetIndex_]/(float)fileEnd_);

    ++offsetIndex_;

    SplitPtr res;
    if(block_->v_) {
        if(splitsProduced_ < 10) {
            std::vector<uint8_t> v = *(block_->v_.get());
            std::string str(v.begin(), v.end());
            LOG(INFO) << "returning split: " << str;
        }
        res = SplitPtr(new Split(block_->v_));
    }
    else if(block_->s_) {
        DLOG_IF(INFO, splitsProduced_ < 10) << "returning split: " << *(block_->s_.get());
        res = SplitPtr(new Split(block_->s_));
    }
    else
        throw std::runtime_error("unable to build block");

    ++splitsProduced_;
    return res;

}

} // namespace splitproducer
} // namespace ddc

