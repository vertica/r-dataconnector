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


#ifndef DDC_RECORDPARSER_CSVRECORDPARSER_H
#define DDC_RECORDPARSER_CSVRECORDPARSER_H

#include <map>
#include <string>
#include <boost/variant.hpp>
#include "base/iobserver.h"
#include "irecordparser.h"
#include "splitproducer/split.h"
#include "text/csv/rows.hpp"

namespace ddc {
namespace recordparser {

typedef boost::variant<bool, int32_t, int64_t,double,std::string> CsvVariant;

struct CsvRecord {
    CsvRecord() : isNull(false)
    {
    }
    explicit CsvRecord(const bool _isNull)
    {
        isNull = _isNull;
    }
    CsvRecord(const bool _isNull, const CsvVariant _value)
    {
        isNull = _isNull;
        value = _value;
    }

    bool isNull;
    CsvVariant value;
};

class CsvRecordParser : public IRecordParser
{
public:
    CsvRecordParser();
    ~CsvRecordParser();

    void configure(base::ConfigurationMap &conf);

    boost::any next();
    bool hasNext();

    base::ConfigurationMap getDebugInfo();



private:
    void dumpDebugInfo();

    splitproducer::Split currentSplit_;
//    std::string line_;
//    std::istringstream ss_;
//    text::csv::csv_istream is_;
    text::csv::row row_;
    int32_t rowIndex_;
    std::map<int32_t, std::pair<std::string,std::string> > schema_;
    splitproducer::ISplitProducerPtr splitProducer_;
    bool configured_;
    uint64_t recordsProduced_;
    char delimiter_;
    char commentCharacter_;
    uint64_t commentLinesDiscarded_;
    uint64_t blankLinesDiscarded_;

    uint64_t chunkStart_;
    uint64_t chunkEnd_;

    std::string url_;
};

} // namespace recordparsing
} // namespace ddc

#endif // DDC_RECORDPARSER_CSVRECORDPARSER_H
