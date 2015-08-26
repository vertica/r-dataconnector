#ifndef DDC_RECORDPARSER_MOCKRECORDPARSER_H
#define DDC_RECORDPARSER_MOCKRECORDPARSER_H

#include <boost/any.hpp>
#include <gmock/gmock.h>
#include "base/iobserver.h"
#include "irecordparser.h"

namespace ddc{
namespace recordparser{
namespace testing{

class MockRecordParser : public IRecordParser
{
public:

    MOCK_METHOD0(hasNext, bool());
    MOCK_METHOD0(next, boost::any());
    MOCK_METHOD1(registerListener, void(base::IObserver<int32_t> *));
    MOCK_METHOD1(configure, void(base::ConfigurationMap &));

};

}//namespace testing
}//namespace recordparser
}//namespace ddc

#endif // DDC_RECORDPARSER_MOCKRECORDPARSER_H
