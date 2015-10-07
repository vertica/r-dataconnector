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
