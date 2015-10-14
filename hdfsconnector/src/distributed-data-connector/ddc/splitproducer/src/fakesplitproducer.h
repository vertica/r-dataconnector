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


#ifndef DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H
#define DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H

#include <string>
#include <vector>
#include "splitproducer/isplitproducer.h"

namespace ddc {
namespace splitproducer {
namespace testing {

class FakeSplitProducer : public ISplitProducer
{
public:
    FakeSplitProducer();
    ~FakeSplitProducer();

    boost::shared_ptr<Split> next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    void setSplits(const std::vector<std::string>& splits);

    base::ConfigurationMap getDebugInfo();

private:
    std::vector<std::string> splits_;
    int32_t index_;


};

} // namespace testing
} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H
