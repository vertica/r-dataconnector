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


#ifndef DDC_ASSEMBLER_FAKEASSEMBLER_H
#define DDC_ASSEMBLER_FAKEASSEMBLER_H

#include <map>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>

#include "iassembler.h"

namespace ddc {
namespace assembler {

class FakeAssembler: public IAssembler
{
public:
    FakeAssembler();
    ~FakeAssembler();

    boost::any getObject();
    void update(int32_t level);

     void configure(base::ConfigurationMap &conf);

private:
    int32_t index_;
    recordparser::IRecordParserPtr recordParser_;
    std::string format_;
    bool configured_;
};

typedef boost::shared_ptr<FakeAssembler> FakeAssemblerPtr;

}  // namespace assembler
}  // namespace ddc

#endif // DDC_ASSEMBLER_FAKEASSEMBLER_H

