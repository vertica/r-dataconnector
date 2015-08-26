#ifndef DDC_BLOCKREADER_FAKEBLOCKREADER_H
#define DDC_BLOCKREADER_FAKEBLOCKREADER_H

#include <string>
#include <vector>
#include "blockreader/iblockreader.h"

namespace ddc{
namespace blockreader {
namespace testing {


class FakeBlockReader : public IBlockReader
{
public:
    FakeBlockReader();
    void setBlocks(const std::vector<std::string>& blocks);

    ~FakeBlockReader();

    void configure(base::ConfigurationMap &conf);

    boost::shared_ptr<Block> next();
    bool hasNext();

    uint64_t blockSize();

    BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes);


private:
    int32_t index_;
    std::vector<std::string> blocks_;
};

} // namespace testing
} // namespace blockreader
} // namespace ddc

#endif // DDC_BLOCKREADER_FAKEBLOCKREADER_H
