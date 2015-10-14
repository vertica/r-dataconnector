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

