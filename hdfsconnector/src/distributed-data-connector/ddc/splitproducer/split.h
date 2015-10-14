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


#ifndef DDC_SPLITPRODUCER_SPLIT_H
#define DDC_SPLITPRODUCER_SPLIT_H

#include <stdint.h>
#include "blockreader/block.h"

namespace ddc {
namespace splitproducer {

struct Split :  public blockreader::Block {
    Split() : Block(){

    }
    Split(uint8_t* b, uint64_t u, uint64_t s) : Block(b,u,s){

    }

    explicit Split(const boost::shared_ptr<std::string>& s) : Block(s) {
    }

    explicit Split(const boost::shared_ptr<std::vector<uint8_t> >& v) : Block(v) {
    }

    //TODO what if buffer wasn't created in this object?
    virtual ~Split() {
    }

};

typedef boost::shared_ptr<Split> SplitPtr;

} // namespace splitproducer
} // namespace ddc
#endif // DDC_SPLITPRODUCER_SPLIT_H

