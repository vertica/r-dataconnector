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


#include "variantmap.h"
#include <boost/shared_ptr.hpp>
#include <gtest/gtest.h>

using namespace std;

namespace base {
namespace testing {

// The fixture for testing class Foo.
class VariantMapTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  VariantMapTest() {
    // You can do set-up work for each test here.
  }

  virtual ~VariantMapTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};




void handleVariant(base::MyVariant &v) {
    switch (v.which()) {
    case 0:
    {

        std::string& str = boost::get<std::string>(v);
        EXPECT_EQ(str,std::string("abc"));
        break;
    }
    case 1:
    {
        int32_t number = boost::get<int32_t>(v);
        EXPECT_EQ(number,123);
        break;
    }
    case 2:
    {
        boost::shared_ptr<base::Date> date = boost::get<boost::shared_ptr<base::Date> >(v);
        EXPECT_EQ(date->hour,2);
        EXPECT_EQ(date->secs,3);
        break;
    }
    default:
    {
        break;
    }
    }
}



TEST_F(VariantMapTest, Basic) {
    base::VariantMap<int32_t> map;
    map.insert(std::make_pair(0, base::MyVariant(std::string("abc"))));
    map.insert(std::make_pair(1, base::MyVariant(123)));
    map.insert(std::make_pair(2, base::MyVariant(boost::shared_ptr<base::Date>(new base::Date(2,3)))));

    base::MyVariant v0 = map.get(0);
    handleVariant(v0);
    base::MyVariant v1 = map.get(1);
    handleVariant(v1);
    base::MyVariant v2 = map.get(2);
    handleVariant(v2);
}

}//namespace testing
}//namespace base

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
