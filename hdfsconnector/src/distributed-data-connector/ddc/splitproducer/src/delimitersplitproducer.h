#ifndef DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H
#define DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H

#include <string>
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


};

} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_DELIMITERSPLITPRODUCER_H
