#include "anymap.h"
#include <boost/shared_ptr.hpp>
#include <gtest/gtest.h>

using namespace std;

namespace base {
namespace testing {

// The fixture for testing class Foo.
class AnyMapTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  AnyMapTest() {
    // You can do set-up work for each test here.
  }

  virtual ~AnyMapTest() {
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


struct Date{
    Date(int h, int s) : hour(h),secs(s) {

    }

    int hour;
    int secs;
};

TEST_F(AnyMapTest, Basic) {
    base::AnyMap<int32_t> map;
    map.insert(std::make_pair(0, boost::any(std::string("abc"))));
    map.insert(std::make_pair(1, boost::any(123)));
    map.insert(std::make_pair(2, boost::any(boost::shared_ptr<Date>(new Date(2,3)))));

    std::string str = boost::any_cast<std::string>(map.get(0));
    EXPECT_EQ(str,std::string("abc"));
    int32_t number = boost::any_cast<int32_t>(map.get(1));
    EXPECT_EQ(number,123);
    boost::shared_ptr<Date> date = boost::any_cast<boost::shared_ptr<Date> >(map.get(2));
    EXPECT_EQ(date->hour,2);
    EXPECT_EQ(date->secs,3);
}

}//namespace testing
}//namespace base


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
