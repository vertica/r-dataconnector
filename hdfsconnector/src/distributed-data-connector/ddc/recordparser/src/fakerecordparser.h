#ifndef DDC_RECORDPARSER_FAKERECORDPARSER_H
#define DDC_RECORDPARSER_FAKERECORDPARSER_H

#include <vector>
#include <boost/any.hpp>
#include "irecordparser.h"

namespace ddc{
namespace recordparser {
namespace testing {


class FakeRecordParser: public IRecordParser
{
public:
    FakeRecordParser();
    ~FakeRecordParser();

    boost::any next();
    bool hasNext();

    void configure(base::ConfigurationMap &conf);

    void setRecords(const std::vector<boost::any>& records);


private:
    int32_t index_;
    std::vector<boost::any> records_;
};

} //namespace testing
} //namespace recordparser
} //namespace ddc
#endif // DDC_RECORDPARSER_FAKERECORDPARSER_H
