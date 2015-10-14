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


#ifndef DDC_BLOCKREADER_BLOCKREADERFACTORY_H
#define DDC_BLOCKREADER_BLOCKREADERFACTORY_H

#include <stdlib.h>

#include <stdexcept>
#include <string>

#include <boost/shared_ptr.hpp>

#include "base/utils.h"
#include "hdfsutils/hdfsutils.h"
#include "blockreader/iblockreader.h"
#include "fakeblockreader.h"
#include "localblockreader.h"
#include "hdfsblockreader.h"
#include "prefetchblockreader.h"


namespace ddc{
namespace blockreader{

class BlockReaderFactory
{
public:
    BlockReaderFactory();
    ~BlockReaderFactory();

    static IBlockReaderPtr makeBlockReader(const std::string& protocol);
};
}//namespace blockreader
}//namespace ddc

#endif // DDC_BLOCKREADER_BLOCKREADERFACTORY_H
