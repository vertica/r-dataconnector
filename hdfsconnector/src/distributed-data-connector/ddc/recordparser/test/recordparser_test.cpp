#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "base/utils.h"

#include "assembler/src/fakeassembler.h"
#include "recordparser/orcrecordparser.h"
#include "recordparser/src/fakerecordparser.h"
#include "splitproducer/src/fakesplitproducer.h"  // TODO get these from factory


using namespace std;
using namespace testing;

namespace ddc{
namespace splitproducer{
namespace testing{

class RecordParserTest : public ::testing::Test {

 protected:

  RecordParserTest() {
  }

  virtual ~RecordParserTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

};


TEST_F(RecordParserTest, Fake) {

    boost::shared_ptr<recordparser::testing::FakeRecordParser> recordParser =
            boost::shared_ptr<recordparser::testing::FakeRecordParser>(new recordparser::testing::FakeRecordParser());
    boost::shared_ptr<splitproducer::testing::FakeSplitProducer> splitProducer =
            boost::shared_ptr<splitproducer::testing::FakeSplitProducer>(new splitproducer::testing::FakeSplitProducer());

    std::vector<boost::any> fakeRecords;
    fakeRecords.push_back(boost::any((int64_t)0));
    fakeRecords.push_back(boost::any(std::string("aaa")));
    fakeRecords.push_back(boost::any((int64_t)0));
    fakeRecords.push_back(boost::any((int64_t)1));
    fakeRecords.push_back(boost::any(std::string("bbb")));
    fakeRecords.push_back(boost::any((int64_t)1));
    fakeRecords.push_back(boost::any((int64_t)2));
    fakeRecords.push_back(boost::any(std::string("ccc")));
    fakeRecords.push_back(boost::any((int64_t)2));
    recordParser->setRecords(fakeRecords);

    std::vector<std::string> splits;
    splits.push_back(std::string("0,aaa,0"));
    splits.push_back(std::string("1,bbb,1"));
    splits.push_back(std::string("2,ccc,2"));
    splitProducer->setSplits(splits);


    base::ConfigurationMap conf;
    conf["splitProducer"] = (splitproducer::ISplitProducerPtr)splitProducer;
    recordParser->configure(conf);

    std::vector<boost::any> records;
    while(recordParser->hasNext()) {
        boost::any record = recordParser->next();
        records.push_back(record);
    }

    std::vector<boost::any> refRecords;
    refRecords.push_back((int64_t)0); refRecords.push_back(std::string("aaa")); refRecords.push_back((int64_t)0);
    refRecords.push_back((int64_t)1); refRecords.push_back(std::string("bbb")); refRecords.push_back((int64_t)1);
    refRecords.push_back((int64_t)2); refRecords.push_back(std::string("ccc")); refRecords.push_back((int64_t)2);

    EXPECT_TRUE(base::utils::areEqual(refRecords,records));
}

struct OrcTestConfig{
    OrcTestConfig(const std::string& _filename,
                  const uint64_t _numRecords,
                  const std::string _selectedStripes) :
        filename(_filename),
        numRecords(_numRecords),
        selectedStripes( _selectedStripes)
    {

    }
    std::string toString() const {
        return str(boost::format("filename: %s, numRecords: %ld selectedStripes: %s")  %
                   filename % numRecords %  selectedStripes);
    }

    std::string filename;
    uint64_t numRecords;
    std::string  selectedStripes;
};

::std::ostream& operator<<(::std::ostream& os, const OrcTestConfig& orcTestConfig) {
    return os << orcTestConfig.toString();  // whatever needed to print bar to os
}


class OrcTest : public ::testing::TestWithParam<OrcTestConfig> {
 protected:

  OrcTest() {
  }

  virtual ~OrcTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

};


TEST_P(OrcTest, General)
{
  // Call GetParam() here to get the Row values
    const OrcTestConfig& p = GetParam();


    assembler::FakeAssembler a;
    recordparser::OrcRecordParser parser;
    parser.registerListener((base::IObserver<int32_t> *)(&a));
    base::ConfigurationMap conf;
    conf["url"] = p.filename;

    std::vector<std::string> selectedStripesStrings;
    boost::split(selectedStripesStrings, p.selectedStripes, boost::is_any_of(","));
    std::vector<uint64_t> selectedStripes;
    if (selectedStripesStrings.size() == 1 && selectedStripesStrings[0] == "") {
        // don't add any stripe
    }
    else {
        for(uint64_t i = 0; i < selectedStripesStrings.size(); ++i) {
            selectedStripes.push_back((uint64_t)atoll(selectedStripesStrings[i].c_str()));
        }
    }
    conf["selectedStripes"] = selectedStripes;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());

    parser.configure(conf);
    uint64_t numRecords = 0;
    while(parser.hasNext()) {
        boost::any record = parser.next();
        ++numRecords;
        if(numRecords % 100000 == 0) {
            DLOG(INFO) << numRecords;
        }
    }
    LOG(INFO) << "numRecords: " << numRecords;

    // print stats
    for(uint64_t i = 0; i < parser.cols(); ++i) {
        LOG(INFO) << "col: " << i <<
                      " nulls: " << parser.nullCount(i);
    }

    EXPECT_EQ(numRecords, p.numRecords);
}



INSTANTIATE_TEST_CASE_P(All, OrcTest, ::testing::Values(
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.test1.orc", 24, "0"),
    OrcTestConfig("hdfs:///orc_files/TestOrcFile.test1.orc", 24, "0"),

    OrcTestConfig("../recordparser/orc/examples/decimal.orc", 6000, "0"),
    OrcTestConfig("../recordparser/orc/examples/nulls-at-end-snappy.orc", 7 * 70000, "0"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testMemoryManagementV12.orc", 2500 * 2, "0,1,2,3"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testStripeLevelStats.orc",11000 * 2, "0,1,2"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testDate1900.orc",70000 * 2, "0,1,2,3,4,5,6,7"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.columnProjection.orc",21000 * 2, "0,1,2,3,4"),
    OrcTestConfig("../recordparser/orc/examples/version1999.orc",0, ""),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testWithoutIndex.orc", 50000 * 2, "0,1,2,3,4,5,6,7,8,9"),
    OrcTestConfig("../recordparser/orc/examples/demo-12-zlib.orc",1920800 * 9, "0"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.metaData.orc", 12, "0"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testMemoryManagementV11.orc", 2500 * 2, "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.emptyFile.orc",0, ""),
    OrcTestConfig("../recordparser/orc/examples/demo-11-zlib.orc",1920800 * 9, "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testPredicatePushdown.orc", 3500 * 2, "0"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testDate2038.orc",212000 * 2, "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27"),
    OrcTestConfig("../recordparser/orc/examples/orc_split_elim.orc", 25000 * 5, "0,1,2,3,4"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testUnionAndTimestamp.orc", 5077 * 3, "0,1"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testSnappy.orc", 10000 * 2, "0,1"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testTimestamp.orc",12, "0"),
    OrcTestConfig("../recordparser/orc/examples/demo-11-none.orc", 1920800 * 9, "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384"),
    OrcTestConfig("../recordparser/orc/examples/over1k_bloom.orc", 2098*11, "0,1"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testStringAndBinaryStatistics.orc", 4 * 2, "0"),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testSeek.orc", 32768 * 12, "0,1,2,3,4,5,6"))
    //OrcTestConfig("../recordparser/orc/examples/orc-file-11-format.orc", 7500 * 14, "0"))  // throws in ORC lib
);

}//namespace testing
}//namespace assembler
}//namespace ddc
