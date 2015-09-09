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

