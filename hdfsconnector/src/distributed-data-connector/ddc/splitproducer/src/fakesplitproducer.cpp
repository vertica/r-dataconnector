#include "fakesplitproducer.h"
#include <iostream>
#include <stdexcept>
#include <glog/logging.h>

namespace ddc {
namespace splitproducer {
namespace testing {

FakeSplitProducer::FakeSplitProducer() : index_(0)
{
//    splits_.push_back(std::string("0,aaa,0"));
//    splits_.push_back(std::string("1,bbb,1"));
//    splits_.push_back(std::string("2,ccc,2"));

}

FakeSplitProducer::~FakeSplitProducer()
{

}

bool FakeSplitProducer::hasNext()
{
    return index_ < 3;

}

void FakeSplitProducer::configure(base::ConfigurationMap &conf)
{

}

void FakeSplitProducer::setSplits(const std::vector<std::string> &splits)
{
    splits_ = splits;
}

base::ConfigurationMap FakeSplitProducer::getDebugInfo() {

}

boost::shared_ptr<Split> FakeSplitProducer::next()
{

    boost::shared_ptr<Split> res = boost::shared_ptr<Split>(new Split(boost::shared_ptr<std::string>(new std::string(splits_[index_]))));
    DLOG(INFO) << "returning split: " << splits_[index_];
    ++index_;
    return res;
}

} // namespace testing
} // namespace splitproducer
} // namespace ddc

