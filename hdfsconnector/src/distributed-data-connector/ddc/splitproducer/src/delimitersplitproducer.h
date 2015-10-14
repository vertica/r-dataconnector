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


#ifndef DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H
#define DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H

#include <string>
#include <utility>
#include <vector>
#include "splitproducer/isplitproducer.h"

namespace ddc {
namespace splitproducer {

class DelimiterSplitProducer : public ISplitProducer
{
public:
    DelimiterSplitProducer();
    ~DelimiterSplitProducer();

    SplitPtr next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    base::ConfigurationMap getDebugInfo();

private:
    blockreader::IBlockReaderPtr blockReader_;
    blockreader::BlockPtr block_;

    std::string split_;
    std::string splitCopy_;

    uint64_t blockSize_;
    uint64_t fileEnd_;
    uint64_t offset_;
    uint64_t offsetInBlock_;
    uint64_t splitEnd_;
    uint64_t splitsDiscarded_;
    uint64_t splitsProduced_;

    uint8_t delimiter_;

    bool configured_;
    bool skipRecord_;
    bool skipHeader_;

    std::vector<std::pair<uint64_t,uint64_t> > requestedBlocks_;


};

} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H
