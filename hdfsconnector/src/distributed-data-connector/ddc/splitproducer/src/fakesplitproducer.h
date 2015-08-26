#ifndef DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H
#define DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H

#include <string>
#include <vector>
#include "splitproducer/isplitproducer.h"

namespace ddc {
namespace splitproducer {
namespace testing {

class FakeSplitProducer : public ISplitProducer
{
public:
    FakeSplitProducer();
    ~FakeSplitProducer();

    boost::shared_ptr<Split> next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    void setSplits(const std::vector<std::string>& splits);

private:
    std::vector<std::string> splits_;
    int32_t index_;


};

} // namespace testing
} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_FAKESPLITPRODUCER_H
