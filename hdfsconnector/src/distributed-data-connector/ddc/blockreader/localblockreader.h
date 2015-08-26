#ifndef DDC_BLOCKREADER_LOCALBLOCKREADER_H
#define DDC_BLOCKREADER_LOCALBLOCKREADER_H

#include <stdio.h>
#include <string>
#include <vector>
#include "base/scopedfile.h"
#include "blockreader/iblockreader.h"

namespace ddc{
namespace blockreader {


class LocalBlockReader : public IBlockReader
{
public:
    LocalBlockReader();
    ~LocalBlockReader();

    void configure(base::ConfigurationMap &conf);

    boost::shared_ptr<Block> next();
    bool hasNext();

    uint64_t blockSize();

    BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);


private:
    uint64_t blockSize_;
    std::string filename_;
    base::ScopedFilePtr f_;
    bool configured_;
};

} // namespace blockreader
} // namespace ddc

#endif // DDC_BLOCKREADER_LOCALBLOCKREADER_H
