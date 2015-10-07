#include "fakerecordparser.h"
#include <iostream>
#include <stdexcept>
#include <string>
#include <glog/logging.h>

namespace ddc{
namespace recordparser {
namespace testing {

FakeRecordParser::FakeRecordParser(): index_(0)
{
    observer_ = NULL;
}

FakeRecordParser::~FakeRecordParser()
{

}

void FakeRecordParser::configure(base::ConfigurationMap &conf)
{
}

void FakeRecordParser::setRecords(const std::vector<boost::any> &records)
{
    records_ = records;
}

base::ConfigurationMap FakeRecordParser::getDebugInfo() {
    return base::ConfigurationMap();
}


bool FakeRecordParser::hasNext() {
    return index_ < 9;
}


boost::any FakeRecordParser::next()
{
    boost::any value = records_[index_];
    if((index_ != 0) && ((index_+1) % 3 == 0)) {
        if(observer_) observer_->update(0);
    }
    DLOG(INFO) <<  "returning index " << index_;
    ++index_;
    return value;
}

}  // namespace testing
}  // namespace recordparser
}  // namespace ddc
