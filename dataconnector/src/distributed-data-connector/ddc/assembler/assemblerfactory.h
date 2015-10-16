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
