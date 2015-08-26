#ifndef DDC_ASSEMBLER_DEVNULLASSEMBLER_H
#define DDC_ASSEMBLER_DEVNULLASSEMBLER_H

#include <map>
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>
#include <Rcpp.h>
#include "iassembler.h"

namespace ddc {
namespace assembler {

class DevNullAssembler: public IAssembler
{
public:
    DevNullAssembler();
    ~DevNullAssembler();

    boost::any getObject();
    void update(int32_t level);

     void configure(base::ConfigurationMap &conf);

private:
    int32_t index_;
    recordparser::IRecordParserPtr recordParser_;
    bool configured_;
};

typedef boost::shared_ptr<DevNullAssembler> DevNullAssemblerPtr;

}  // namespace assembler
}  // namespace ddc

#endif // DDC_ASSEMBLER_DEVNULLASSEMBLER_H
