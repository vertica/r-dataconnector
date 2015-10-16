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


#include "hdfsutils.h"
#include <gtest/gtest.h>

namespace ddc {
namespace hdfsutils {
namespace testing {

class HdfsUtilsTest : public ::testing::Test {
 protected:

  HdfsUtilsTest() {
  }

  virtual ~HdfsUtilsTest() {
  }

  virtual void SetUp() {
      base::ConfigurationMap conf;
      conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
      globber_.configure(conf);
  }

  virtual void TearDown() {
  }

  std::string getRootWrapper(const std::string& s) {
      return globber_.getRoot(s);
  }

  bool isFileWrapper(const std::string& s) {
      return globber_.isFile(s);
  }
  std::vector<std::string> listDirWrapper(const std::string& s) {
      return globber_.listDir(s);
  }

  void walkWrapper(const std::string& s,
                   std::vector<std::string>& files) {
      return globber_.walk(s, files);
  }

  HdfsGlobber globber_;
};

TEST_F(HdfsUtilsTest, Root) {

    EXPECT_EQ(std::string("a/b"), getRootWrapper("a/b/*.txt"));
}

TEST_F(HdfsUtilsTest, IsFile) {
    EXPECT_FALSE(isFileWrapper("/"));
    EXPECT_TRUE(isFileWrapper("/ex001.csv"));
}

std::ostream& operator<<(std::ostream& os,
                         const std::vector<std::string>& v) {

    for (uint64_t i = 0; i < v.size(); ++i) {
        os << v[i] << ",";
    }
    return os;
}

TEST_F(HdfsUtilsTest, ListDir) {
    std::vector<std::string> files = listDirWrapper("/");
    DLOG(INFO) << files;
}

TEST_F(HdfsUtilsTest, Walk) {
    std::vector<std::string> files;
    walkWrapper("/", files);
    DLOG(INFO) << files;
}

TEST_F(HdfsUtilsTest, Glob) {
    std::vector<std::string> files = globber_.glob("/*.csv");
    std::vector<std::string> files2 = globber_.glob("/*/*.orc");
    DLOG(INFO) << files;
    DLOG(INFO) << files2;
}

}  // namespace testing
}  // namespace hdfsutils
}  // namespace ddc
