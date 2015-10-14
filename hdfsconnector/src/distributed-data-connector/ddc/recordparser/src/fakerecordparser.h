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


#ifndef DDC_RECORDPARSER_FAKERECORDPARSER_H
#define DDC_RECORDPARSER_FAKERECORDPARSER_H

#include <vector>
#include <boost/any.hpp>
#include "irecordparser.h"

namespace ddc{
namespace recordparser {
namespace testing {


class FakeRecordParser: public IRecordParser
{
public:
    FakeRecordParser();
    ~FakeRecordParser();

    boost::any next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    void setRecords(const std::vector<boost::any>& records);

    base::ConfigurationMap getDebugInfo();

private:
    int32_t index_;
    std::vector<boost::any> records_;
};

} //namespace testing
} //namespace recordparser
} //namespace ddc
#endif // DDC_RECORDPARSER_FAKERECORDPARSER_H
