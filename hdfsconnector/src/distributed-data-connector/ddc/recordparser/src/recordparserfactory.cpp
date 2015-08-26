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
