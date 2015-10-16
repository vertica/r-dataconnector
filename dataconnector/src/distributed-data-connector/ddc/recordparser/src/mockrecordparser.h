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

    MOCK_METHOD0(getDebugInfo, base::ConfigurationMap());

};

}//namespace testing
}//namespace recordparser
}//namespace ddc

#endif // DDC_RECORDPARSER_MOCKRECORDPARSER_H
