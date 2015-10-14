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


#ifndef DDC_BLOCKREADER_BLOCK_H
#define DDC_BLOCKREADER_BLOCK_H

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

namespace ddc {
namespace blockreader {

struct Block {
    Block() : buffer(NULL), used(0), size(0){

    }
    Block(uint8_t* b, uint64_t u, uint64_t s) : buffer(b), used(u), size(s){

    }

    explicit Block(const boost::shared_ptr<std::string>& s) {
        s_ = s;
        buffer = (uint8_t *)s->data();
        used = s->size();
        size = s->size();
    }

    explicit Block(const boost::shared_ptr<std::vector<uint8_t> >& v) {
        v_ = v;
        buffer = (uint8_t *)v->data();
        used = v->size();
        size = v->size();
    }

    virtual ~Block() {
    }


    uint8_t *buffer;
    uint64_t used;
    uint64_t size;


    boost::shared_ptr<std::string> s_;
    boost::shared_ptr<std::vector<uint8_t> > v_;

private:

};

typedef boost::shared_ptr<Block> BlockPtr;

} // namespace blockreader
} // namespace ddc
#endif // DDC_BLOCKREADER_BLOCK_H

