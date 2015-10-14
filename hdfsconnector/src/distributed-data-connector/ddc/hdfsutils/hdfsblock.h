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


#ifndef DDC_HDFSUTILS_HDFSBLOCK_H
#define DDC_HDFSUTILS_HDFSBLOCK_H

#include <stdio.h>
#include <string.h>

#include "yajl/yajl_tree.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>


#include <glog/logging.h>

namespace ddc {
namespace hdfsutils {

struct HdfsBlock{
    size_t blockId;
    size_t startOffset;
    size_t numBytes;
    std::vector<std::string> locations; //ip addresses of DNs that have the block

    bool operator==(const HdfsBlock& other) {

        if(blockId != other.blockId) {
            DLOG(INFO) << "different blockId " << blockId << " vs " << other.blockId;
            return false;
        }
        if(startOffset != other.startOffset) return false;
        if(numBytes != other.numBytes) return false;
        if(locations.size() != other.locations.size()) return false;

        std::vector<std::string> copy(locations);
        std::sort(copy.begin(), copy.end());
        std::vector<std::string> otherCopy(other.locations);
        std::sort(otherCopy.begin(), otherCopy.end());
        if(copy != otherCopy) {
            DLOG(INFO) << "different locations";
            return false;
        }


        return true;
    }

    void print() {
        printf("blockId: %zu, startOffset: %10zu, numBytes: %zu ", blockId, startOffset, numBytes);
        printf("locations: ");
        for (uint64_t i=0; i < locations.size(); i++) {
            printf("%s, ", locations[i].c_str());
        }
        printf("\n");
    }
};

struct Range {
    uint64_t start;
    uint64_t end;
};

struct HdfsBlockRange {
    HdfsBlock block;
    Range range;
};

} // namespace hdfsutils
} // namespace ddc

#endif // DDC_HDFSUTILS_HDFSBLOCK_H
