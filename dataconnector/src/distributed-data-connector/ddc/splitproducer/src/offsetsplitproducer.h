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


#ifndef DDC_SPLITPRODUCER_OFFSETSPLITPRODUCER_H
#define DDC_SPLITPRODUCER_OFFSETSPLITPRODUCER_H

#include <string>
#include <vector>
#include "splitproducer/isplitproducer.h"

namespace ddc {
namespace splitproducer {

class OffsetSplitProducer : public ISplitProducer
{
public:
    OffsetSplitProducer();
    ~OffsetSplitProducer();

    SplitPtr next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    base::ConfigurationMap getDebugInfo();

private:

    blockreader::IBlockReaderPtr blockReader_;
    blockreader::BlockPtr block_;

    std::vector<uint64_t> offsets_;

    uint64_t blockSize_;
    uint64_t fileEnd_;
    uint64_t offsetIndex_;
    uint64_t splitsProduced_;
    uint64_t splitsDiscarded_;

    bool configured_;
};

} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_OFFSETSPLITPRODUCER_H
