#include <gtest/gtest.h>

#include "base/cache.h"

namespace base {
namespace testing {

    TEST(cache, simple) {
        Cache c;
        EXPECT_FALSE(c.contains("key"));
        c.set("key", std::string("value"));
        EXPECT_TRUE(c.contains("key"));
        EXPECT_EQ(boost::any_cast<std::string>(c.get("key")), std::string("value"));
    }
}  // namespace testing
}  // namespace base

