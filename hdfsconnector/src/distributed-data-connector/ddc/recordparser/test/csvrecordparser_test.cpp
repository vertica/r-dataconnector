#include <utility>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "base/scopedfile.h"
#include "base/utils.h"
#include "ddc/ddc.h"
#include "assembler/assemblerfactory.h"
#include "fakesplitproducer.h"
#include "blockreader/blockreaderfactory.h"
#include "recordparser/csvrecordparser.h"
#include "recordparser/recordparserfactory.h"
#include "splitproducer/splitproducerfactory.h"

using namespace std;

namespace ddc{
namespace recordparser{
namespace testing{

// The fixture for testing class Foo.
class CsvRecordParserTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  CsvRecordParserTest() {
    // You can do set-up work for each test here.
  }

  virtual ~CsvRecordParserTest() {
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


class MyObserver : public base::IObserver<int32_t>{
public:
    void update(int32_t res) {
        DLOG(INFO) << "split completed!";
    }
};

TEST_F(CsvRecordParserTest, Basic) {
    boost::shared_ptr<recordparser::IRecordParser> recordParser = recordparser::RecordParserFactory::makeRecordParser(std::string("csv"));
    //boost::shared_ptr<splitproducer::ISplitProducer> splitProducer = splitproducer::SplitProducerFactory::makeSplitProducer(std::string("fake"));
    boost::shared_ptr<splitproducer::testing::FakeSplitProducer> splitProducer =
            boost::shared_ptr<splitproducer::testing::FakeSplitProducer>(new splitproducer::testing::FakeSplitProducer());

    std::vector<std::string> splits;
    splits.push_back(std::string("0,aaa,0.0"));
    splits.push_back(std::string("1,bbb,1.0"));
    splits.push_back(std::string("2,ccc,2.0"));
    splitProducer->setSplits(splits);

    base::ConfigurationMap conf;
    conf["splitProducer"] = (splitproducer::ISplitProducerPtr)splitProducer;
    std::map<int32_t, std::pair<std::string,std::string> > schema;
    schema[0] = std::make_pair("a","integer");
    schema[1] = std::make_pair("b","character");
    schema[2] = std::make_pair("c","numeric");
    conf["schema"] = schema;
    conf["delimiter"] = ',';
    conf["commentCharacter"] = '#';
    recordParser->configure(conf);
    MyObserver o;
    recordParser->registerListener(&o);

    std::vector<boost::any> records;
    uint64_t i = 0;
    while(recordParser->hasNext()) {
        boost::any record = recordParser->next();
        recordparser::CsvRecord csvRecord = boost::any_cast<recordparser::CsvRecord>(record);
        if (i % 3 == 0)
            records.push_back(boost::get<int32_t>(csvRecord.value));
        else if (i % 3 == 1)
            records.push_back(boost::get<std::string>(csvRecord.value));
        else if (i % 3 == 2)
            records.push_back(boost::get<double>(csvRecord.value));
        i += 1;
    }

    std::vector<boost::any> refRecords;
    refRecords.push_back((int32_t)0); refRecords.push_back(std::string("aaa")); refRecords.push_back((double)0.0);
    refRecords.push_back((int32_t)1); refRecords.push_back(std::string("bbb")); refRecords.push_back((double)1.0);
    refRecords.push_back((int32_t)2); refRecords.push_back(std::string("ccc")); refRecords.push_back((double)2.0);

    std::cout << "read " << records.size() << " records" << std::endl;
    EXPECT_TRUE(base::utils::areEqual(records, refRecords));
}

struct CsvRecordParserConfig{
    CsvRecordParserConfig(const std::string& f, const std::string& s,
                             const uint64_t nl, const uint64_t nc):
        filename(f), schema(s), numLines(nl), numCols(nc) {
    }
    std::string filename;
    std::string schema;
    uint64_t numLines;
    uint64_t numCols;
};

class CsvRecordParserParamTest : public ::testing::TestWithParam<CsvRecordParserConfig> {

};

TEST_P(CsvRecordParserParamTest, Generic){
    const CsvRecordParserConfig& p = GetParam();

    std::string filename = p.filename;

    base::ScopedFile file(filename);
    blockreader::IBlockReaderPtr b = blockreader::BlockReaderFactory::makeBlockReader(base::utils::getExtension(filename));
    splitproducer::ISplitProducerPtr s = splitproducer::SplitProducerFactory::makeSplitProducer(base::utils::getExtension(filename));
    IRecordParserPtr r = RecordParserFactory::makeRecordParser(base::utils::getExtension(filename));

    base::ConfigurationMap blockReaderConf;
    blockReaderConf["blocksize"] = (uint64_t)(32 * 1024 * 1024);
    blockReaderConf["filename"] = filename;
    b->configure(blockReaderConf);

    base::ConfigurationMap splitProducerConf;
    splitProducerConf["delimiter"] = (uint8_t)'\n';
    splitProducerConf["splitStart"] = (uint64_t)0;
    splitProducerConf["splitEnd"] = file.stat().length;
    splitProducerConf["fileEnd"] = file.stat().length;
    splitProducerConf["blockReader"] = b;
    s->configure(splitProducerConf);

    base::ConfigurationMap recordParserConf;
    recordParserConf["splitProducer"] = s;
    std::string schema = p.schema;
    recordParserConf["schema"] = parseSchema(schema);
    recordParserConf["delimiter"] = ',';
    recordParserConf["commentCharacter"] = '#';
    r->configure(recordParserConf);

    assembler::IAssemblerPtr fakeAssembler = assembler::AssemblerFactory::makeAssembler("fake");
    r->registerListener(fakeAssembler.get());

    std::vector<boost::any> records;
    while(r->hasNext()) {
        boost::any record = r->next();
        records.push_back(record);
    }


    EXPECT_EQ(records.size(), p.numLines * p.numCols);
    recordparser::CsvRecord csvRecord =
            boost::any_cast<recordparser::CsvRecord>(records[0]);
    double value = boost::get<double>(csvRecord.value);
    EXPECT_TRUE(base::utils::areEqual(value, (double)0));
    recordparser::CsvRecord csvRecord2 =
            boost::any_cast<recordparser::CsvRecord>(records[records.size() - 1]);
    double value2 = boost::get<double>(csvRecord2.value);
    EXPECT_TRUE(base::utils::areEqual(value2, (double)(records.size() - 1)));


}

INSTANTIATE_TEST_CASE_P(All, CsvRecordParserParamTest, ::testing::Values(
    CsvRecordParserConfig("../ddc/test/data/test4MB.csv",
                          "000:int64,"
                          "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
                          "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64",
                          16 * 1024,
                          16)
));

} // namespace testing
} // namespace recordparser
} // namespace ddc
