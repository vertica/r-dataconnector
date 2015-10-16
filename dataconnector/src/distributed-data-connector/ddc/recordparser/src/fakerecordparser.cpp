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
