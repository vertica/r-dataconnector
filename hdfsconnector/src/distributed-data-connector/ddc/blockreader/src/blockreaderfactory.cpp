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


#include "blockreaderfactory.h"

using boost::shared_ptr;
using std::runtime_error;
using std::string;

namespace ddc{
namespace blockreader{

BlockReaderFactory::BlockReaderFactory()
{

}

BlockReaderFactory::~BlockReaderFactory()
{

}



IBlockReaderPtr BlockReaderFactory::makeBlockReader(const std::string& protocol){

    if(protocol == "fake") return IBlockReaderPtr(new testing::FakeBlockReader());

    char* enablePrefetching = getenv("DDC_ENABLE_PREFETCHING");
    if (enablePrefetching != NULL) {
        LOG(WARNING) << "Prefetching is enabled ...";
        return IBlockReaderPtr(new PrefetchBlockReader());
    }
    else {
        if(hdfsutils::isHdfs(protocol)) {
            return IBlockReaderPtr(new HdfsBlockReader());
        }
        else if( protocol == "file")  {
            return IBlockReaderPtr(new LocalBlockReader());
        }
        else { //no protocol, return local reader
            return IBlockReaderPtr(new LocalBlockReader());
        }
    }
}
}//namespace blockreader
}//namespace ddc
