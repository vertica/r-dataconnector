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
