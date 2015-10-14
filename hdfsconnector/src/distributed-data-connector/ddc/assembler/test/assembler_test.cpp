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


#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <Rcpp.h>
#include <RInside.h>

#include "assembler/assemblerfactory.h"
#include "assembler/rdataframeassembler.h"
#include "fakerecordparser.h"
#include "recordparser/recordparserfactory.h"

using namespace std;
using namespace Rcpp;
using namespace testing;

namespace ddc{
namespace assembler{
namespace testing{

// The fixture for testing class Foo.
class FakeAssemblerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FakeAssemblerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~FakeAssemblerTest() {
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

  void handleOrcRecordWrapper(RDataFrameAssembler& assembler, boost::any& any) {
        assembler.handleOrcRecord(any);
  }
};


// [[Rcpp::export]]
DataFrame getDataFrame() {
    // and access each column by name
    NumericVector a(3);
    CharacterVector b(3);
    DoubleVector c(3);
    // do something
    a[0] = 0; a[1] = 1; a[2] = 2;
    b[0] = "aaa"; b[1] = "bbb"; b[2] = "ccc";
    c[0] = 0.0; c[1] = 1.0; c[2] = 2.0;
    // create a new data frame
    DataFrame df =
        DataFrame::create(
            Named("a")=a,
            Named("b")=b,
            Named("c")=c
        );
    return df;
}



class AssemblerWrapper {
  public:
    explicit AssemblerWrapper(IAssembler* assembler) : assembler_(assembler){

    }

    boost::any update() {
        int LINE_CONSUMED = 0;
        assembler_->update(LINE_CONSUMED);
        return boost::any();
    }

    private:
        IAssembler *assembler_;

};



bool dataframesAreEqual(Rcpp::DataFrame &a, Rcpp::DataFrame &b) {
    Rcpp::NumericVector a1 = a["a"];
    Rcpp::CharacterVector b1 = a["b"];
    Rcpp::DoubleVector c1 = a["c"];

    Rcpp::NumericVector a2 = b["a"];
    Rcpp::CharacterVector b2 = b["b"];
    Rcpp::DoubleVector c2 = b["c"];

    if(a.size() != b.size()) {
        DLOG(INFO) << "sizes different";
        return false;
    }
    for(int i = 0; i < a1.size(); i++) {
        if(a1[i] != a2[i]) {
            DLOG(INFO) << a1[i] << " vs " << a2[i];
            return false;
        }
        if(b1[i] != b2[i]) {
            DLOG(INFO) << b1[i] << " vs " << b2[i];
            return false;
        }
        if(c1[i] != c2[i]) {
            DLOG(INFO) << c1[i] << " vs " << c2[i];
            return false;
        }
    }
    return true;
}

void helper(const std::vector<boost::any>& records, const std::string& format) {
    boost::shared_ptr<assembler::IAssembler> assembler = assembler::AssemblerFactory::makeAssembler(std::string("fake"));
    //boost::shared_ptr<recordparser::IRecordParser> recordParser = recordparser::RecordParserFactory::makeRecordParser(std::string("fake"));
    boost::shared_ptr<recordparser::testing::FakeRecordParser> recordParser =
            boost::shared_ptr<recordparser::testing::FakeRecordParser>(new recordparser::testing::FakeRecordParser());

    recordParser->setRecords(records);

    DataFrame referenceDataFrame = getDataFrame();
    base::ConfigurationMap conf;
    conf["recordParser"] = (recordparser::IRecordParserPtr)recordParser;
    conf["format"] = format;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    assembler->configure(conf);
    Rcpp::DataFrame df = boost::any_cast<Rcpp::DataFrame>(assembler->getObject());

    EXPECT_TRUE(dataframesAreEqual(referenceDataFrame, df));
}


TEST_F(FakeAssemblerTest, Fake) {
    std::vector<boost::any> records;
    records.push_back(boost::any((int32_t)0));
    records.push_back(boost::any(std::string("aaa")));
    records.push_back(boost::any((double)0.0));
    records.push_back(boost::any((int32_t)1));
    records.push_back(boost::any(std::string("bbb")));
    records.push_back(boost::any((double)1.0));
    records.push_back(boost::any((int32_t)2));
    records.push_back(boost::any(std::string("ccc")));
    records.push_back(boost::any((double)2.0));

    helper(records, "row");
}

TEST_F(FakeAssemblerTest, FakeColumnar) {
    std::vector<boost::any> records;
    records.push_back(boost::any((int32_t)0));
    records.push_back(boost::any((int32_t)1));
    records.push_back(boost::any((int32_t)2));
    records.push_back(boost::any(std::string("aaa")));
    records.push_back(boost::any(std::string("bbb")));
    records.push_back(boost::any(std::string("ccc")));
    records.push_back(boost::any((double)0.0));
    records.push_back(boost::any((double)1.0));
    records.push_back(boost::any((double)2.0));

    helper(records, "column");
}

TEST_F(FakeAssemblerTest, OrcTimestamp) {
    RDataFrameAssembler a;


    recordparser::IRecordParserPtr r =
            recordparser::RecordParserFactory::makeRecordParser("orc");


    base::ConfigurationMap recordParserConf;
    std::string filename = "../recordparser/orc/examples/TestOrcFile.testTimestamp.orc";
    recordParserConf["url"] = filename;
    std::vector<uint64_t> selectedStripes;
    selectedStripes.push_back(0);
    recordParserConf["selectedStripes"] = selectedStripes;
    r->configure(recordParserConf);

    base::ConfigurationMap assemblerConf;
    assemblerConf["url"] = filename;
    assemblerConf["format"] = std::string("column");
    assemblerConf["recordParser"] = r;

    a.configure(assemblerConf);

    std::vector<recordparser::NodePtr> strings;
    for(int i = 0; i < 10; ++i) {
        recordparser::NodePtr node(new recordparser::Node);
        node->type = orc::STRING;
        node->value = base::utils::to_string(i);
        strings.push_back(node);
    }

    for(int i = 0; i < 10; i++) {
        boost::any record(strings[i]);
        handleOrcRecordWrapper(a, record);
    }

    // the test is that is finishes without errors or exceptions
}


}//namespace testing
}//namespace assembler
}//namespace ddc

