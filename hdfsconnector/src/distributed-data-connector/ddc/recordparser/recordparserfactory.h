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


#ifndef DDC_RECORPARSER_RECORDPARSERFACTORY_H
#define DDC_RECORPARSER_RECORDPARSERFACTORY_H

#include <string>
#include <boost/shared_ptr.hpp>
#include "irecordparser.h"

namespace ddc{
namespace recordparser{
class RecordParserFactory
{
public:
    RecordParserFactory();
    ~RecordParserFactory();

    static boost::shared_ptr<IRecordParser> makeRecordParser(const std::string& fileExtension);
};
}//namespace recordparser
}//namespace ddc

#endif // DDC_RECORPARSER_RECORDPARSERFACTORY_H
