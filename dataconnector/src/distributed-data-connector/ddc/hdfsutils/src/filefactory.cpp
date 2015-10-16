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


#include "filefactory.h"


using boost::make_shared;
using boost::shared_ptr;
using std::runtime_error;
using std::string;

namespace ddc{
namespace hdfsutils {

FileFactory::FileFactory()
{

}

FileFactory::~FileFactory()
{

}



base::IFilePtr FileFactory::makeFile(const std::string& protocol,
                                     const std::string& filename,
                                     const std::string& mode){

    if(protocol == "fake") {
        throw std::runtime_error("unsupported");
    }
    else if(isHdfs(protocol)) {
        return base::IFilePtr(new HdfsFile(filename));
    }
    else if( protocol == "file")  {
        return base::IFilePtr(new base::ScopedFile(filename, mode));
    }
    else { //no protocol, return local reader
        return base::IFilePtr(new base::ScopedFile(filename, mode));
    }
}

}//namespace hdfsutils
}//namespace ddc
