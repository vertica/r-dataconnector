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


#include "recordparserfactory.h"
#include <stdexcept>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "base/utils.h"
#include "csvrecordparser.h"
#include "fakerecordparser.h"
#include "mockrecordparser.h"
#include "orcrecordparser.h"

using boost::make_shared;
using boost::shared_ptr;
using std::runtime_error;
using std::string;

namespace ddc{
namespace recordparser{
RecordParserFactory::RecordParserFactory()
{

}

RecordParserFactory::~RecordParserFactory()
{

}



shared_ptr<IRecordParser> RecordParserFactory::makeRecordParser(const std::string & fileExtension){

    if(fileExtension== "fake") return shared_ptr<testing::FakeRecordParser>(new testing::FakeRecordParser());
    else if(fileExtension == "mock") return shared_ptr<testing::MockRecordParser>(new testing::MockRecordParser());
    else if(fileExtension == "csv") return shared_ptr<CsvRecordParser>(new CsvRecordParser());
    else if(fileExtension == "offsetcsv") return shared_ptr<CsvRecordParser>(new CsvRecordParser());
    else if(fileExtension == "orc") return shared_ptr<OrcRecordParser>(new OrcRecordParser());
    else throw runtime_error(string("No recordparser class found for file extension ") + fileExtension);
}
}//namespace recordparser
}//namespace ddc
