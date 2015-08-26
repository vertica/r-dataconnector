#ifndef DDC_ASSEMBLER_ASSEMBLERFACTORY_H
#define DDC_ASSEMBLER_ASSEMBLERFACTORY_H

#include <boost/shared_ptr.hpp>
#include "iassembler.h"

namespace ddc{
namespace assembler {

/**
 * @brief Factory class to create different kinds of assemblers.
 */
class AssemblerFactory
{
public:
    AssemblerFactory();
    ~AssemblerFactory();

    /**
     * @brief makeAssembler
     * @param objectType Type of the assembler object. E.g. "rdataframe"
     * @return
     */
    static boost::shared_ptr<IAssembler> makeAssembler(const std::string& objectType);
};
}//namespace assembler
}//namespace ddc

#endif // DDC_ASSEMBLER_ASSEMBLERFACTORY_H
