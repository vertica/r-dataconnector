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
    using boost::any_cast;

    fileEnd_ = any_cast<uint64_t>(conf["fileEnd"]);
    offsets_ = any_cast<std::vector<uint64_t> >(conf["offsets"]);
    blockReader_ = any_cast<blockreader::IBlockReaderPtr>(conf["blockReader"]);
    offsetIndex_ = 0;
    configured_ = true;

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

