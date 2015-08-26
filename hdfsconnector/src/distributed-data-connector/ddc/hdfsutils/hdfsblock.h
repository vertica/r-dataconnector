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
