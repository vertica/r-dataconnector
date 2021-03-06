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


#include <cstdlib>

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/format.hpp>
#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <Rcpp.h>
#include <RInside.h>

#include "ddc/ddc.h"
#include "base/ifile.h"
#include "base/utils.h"
#include "hdfsutils/filefactory.h"
#include "hdfsutils/hdfsinputstream.h"
#include "hdfsutils/hdfsutils.h"
#include "orc/Reader.hh"

namespace ddc {
namespace testing {

class DdcTest : public ::testing::Test {
 protected:

  DdcTest() {
  }

  virtual ~DdcTest() {
  }

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};


/**
 * High level tests:
 *
 * Modes: read/write
 * Output objects: Rcpp::DataFrame, Rcpp::Array
 * Storage layers: Local, HDFS
 * Formats: CSV, JSON, Avro, Parquet, ORC
 * Configuration options: IO thread enabled, num retries per datanode.
 *
 */

//
// Creates a golden DataFrame
//
//   a  |  b
// -----------
//   1  |  aaa
//   2  |  bbb
//   3  |  ccc
//
Rcpp::DataFrame getDataFrame() {
    // and access each column by name
    Rcpp::NumericVector a(3);
    Rcpp::CharacterVector b(3);
    //DateVector c(3);
    // do something
    a[0] = 1; a[1] = 2; a[2] = 3;
    b[0] = "aaa"; b[1] = "bbb"; b[2] = "ccc";
    Rcpp::DataFrame df =
        Rcpp::DataFrame::create(
            Rcpp::Named("a")=a,
            Rcpp::Named("b")=b
        );
    return df;
}

// checks if 2 dataframes are equal
// only dataframes with 2 columns (a: int, b: string) are supported
bool dataframesAreEqual(Rcpp::DataFrame &a, Rcpp::DataFrame &b) {
    Rcpp::NumericVector a1 = a["a"];
    Rcpp::CharacterVector b1 = a["b"];

    Rcpp::NumericVector a2 = b["a"];
    Rcpp::CharacterVector b2 = b["b"];

    for(int i = 0; i < a1.size(); i++) {
        if(a1[i] != a2[i]) {
            DLOG(INFO) << a1[i] << " vs " << a2[i];
            return false;
        }
        if(b1[i] != b2[i]) {
            DLOG(INFO) << b1[i] << " vs " << b2[i];
            return false;
        }
    }
    return true;
}


typedef boost::shared_ptr<Rcpp::DataFrame> DataFramePtr;
typedef boost::shared_ptr<Rcpp::List> ListPtr;


TEST_F(DdcTest, LocalEx001Csv)
{

    std::string url = "../ddc/test/data/ex001.csv";
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, url, "r");
    base::FileStatus status = file->stat();

    Rcpp::DataFrame referenceDataFrame = getDataFrame();
    base::ConfigurationMap conf;
    conf["schemaUrl"] = std::string("a:int64,b:character");
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    ListPtr listptr = boost::any_cast<DataFramePtr>(ddc_read(url,
                                                        "rdataframe",
                                                        conf));
    Rcpp::List list = *(listptr.get());
    Rcpp::DataFrame df = Rcpp::DataFrame(list);
    EXPECT_TRUE(dataframesAreEqual(df, referenceDataFrame));
}


TEST_F(DdcTest, HdfsEx001Csv)
{
    std::string url = "hdfs:///ex001.csv";
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);

    std::string filename = base::utils::stripProtocol(url);

    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename, "r");
    if(protocol == "hdfs") {
        hdfsutils::HdfsFile *p = (hdfsutils::HdfsFile *)file.get();
        base::ConfigurationMap conf;
        conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
        conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
        p->configure(conf);
    }
    base::FileStatus status = file->stat();

    Rcpp::DataFrame referenceDataFrame = getDataFrame();
    base::ConfigurationMap conf;
    conf["schemaUrl"] = std::string("a:int64,b:character");
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    ListPtr listptr = boost::any_cast<DataFramePtr>(ddc_read(url,
                                                        "rdataframe",
                                                        conf));
    Rcpp::List list = *(listptr.get());
    Rcpp::DataFrame df = Rcpp::DataFrame(list);
    EXPECT_TRUE(dataframesAreEqual(df, referenceDataFrame));
}

//
// Creates a golden DataFrame
//
//   a  |  b    |  c  |  d
// ---------------------------
//   1  |  aaa  |  1  | aaa
//   2  |  bbb  |  2  | bbb
//   3  |  ccc  |  3  | ccc
//
Rcpp::DataFrame getDataFrame2() {
    // and access each column by name
    Rcpp::CharacterVector a(3);
    Rcpp::NumericVector b(3);
    Rcpp::CharacterVector c(3);
    Rcpp::NumericVector d(3);

    //DateVector c(3);
    // do something
    b[0] = 1; b[1] = 2; b[2] = 3;
    d[0] = 1; d[1] = 2; d[2] = 3;
    a[0] = "aaa"; a[1] = "bbb"; a[2] = "ccc";
    c[0] = "aaa"; c[1] = "bbb"; c[2] = "ccc";
    //c[0] = 0; c[1] = 1; c[2] = 2;
    // create a new data frame
    Rcpp::DataFrame df =
        Rcpp::DataFrame::create(
            Rcpp::Named("a")=a,
            Rcpp::Named("b")=b,
            Rcpp::Named("c")=c,
            Rcpp::Named("d")=d
        );
    return df;
}

// checks if 2 dataframes are equal
// only dataframes with 4 columns (a: int, b: string, c: int, d: string) are supported
bool dataframesAreEqual2(Rcpp::DataFrame &a, Rcpp::DataFrame &b) {

    Rcpp::CharacterVector a1 = a["a"];
    Rcpp::NumericVector b1 = a["b"];
    Rcpp::CharacterVector c1 = a["c"];
    Rcpp::NumericVector d1 = a["d"];

    Rcpp::CharacterVector a2 = b["a"];
    Rcpp::NumericVector b2 = b["b"];
    Rcpp::CharacterVector c2 = b["c"];
    Rcpp::NumericVector d2 = b["d"];

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
        if(d1[i] != d2[i]) {
            DLOG(INFO) << d1[i] << " vs " << d2[i];
            return false;
        }
    }
    return true;
}


TEST_F(DdcTest, LocalEx002Csv)
{
    std::string url = "../ddc/test/data/ex002.csv";
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, url, "r");
    base::FileStatus status = file->stat();


    Rcpp::DataFrame referenceDataFrame = getDataFrame2();
    base::ConfigurationMap conf;
    conf["schemaUrl"] = std::string("a:character,b:int64,c:character,d:int64");
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    ListPtr listptr = boost::any_cast<DataFramePtr>(ddc_read(url,
                                                        "rdataframe",
                                                        conf));
    Rcpp::List list = *(listptr.get());
    Rcpp::DataFrame df = Rcpp::DataFrame(list);
    EXPECT_TRUE(dataframesAreEqual2(df, referenceDataFrame));

}


TEST_F(DdcTest, HdfsEx002Csv)
{
    std::string url = "hdfs:///ex002.csv";
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    std::string filename = "/ex002.csv";
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename, "r");
    if(protocol == "hdfs") {
        hdfsutils::HdfsFile *p = (hdfsutils::HdfsFile *)file.get();
        base::ConfigurationMap conf;
        conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
        conf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
        p->configure(conf);
    }
    base::FileStatus status = file->stat();

    Rcpp::DataFrame referenceDataFrame = getDataFrame2();
    base::ConfigurationMap conf;
    conf["schemaUrl"] = std::string("a:character,b:int64,c:character,d:int64");
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    ListPtr listptr = boost::any_cast<DataFramePtr>(ddc_read(url,
                                                        "rdataframe",
                                                        conf));
    Rcpp::List list = *(listptr.get());
    Rcpp::DataFrame df = Rcpp::DataFrame(list);
    EXPECT_TRUE(dataframesAreEqual2(df, referenceDataFrame));
}

TEST_F(DdcTest, DISABLED_HdfsTest512Csv)
{
    std::string url = "hdfs:///test512MB.csv";
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    std::string filename = "/test512MB.csv";
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename, "r");
    if(protocol == "hdfs") {
        hdfsutils::HdfsFile *p = (hdfsutils::HdfsFile *)file.get();
        base::ConfigurationMap conf;
        conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
        p->configure(conf);
    }
    base::FileStatus status = file->stat();

    Rcpp::DataFrame referenceDataFrame = getDataFrame2();
    base::ConfigurationMap conf;
    conf["schemaUrl"] = std::string(
          "000:int64,"
          "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
          "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64,016:int64,"
          "017:int64,018:int64,019:int64,020:int64,021:int64,022:int64,023:int64,024:int64,"
          "025:int64,026:int64,027:int64,028:int64,029:int64,030:int64,031:int64,032:int64,"
          "033:int64,034:int64,035:int64,036:int64,037:int64,038:int64,039:int64,040:int64,"
          "041:int64,042:int64,043:int64,044:int64,045:int64,046:int64,047:int64,048:int64,"
          "049:int64,050:int64,051:int64,052:int64,053:int64,054:int64,055:int64,056:int64,"
          "057:int64,058:int64,059:int64,060:int64,061:int64,062:int64,063:int64,064:int64,"
          "065:int64,066:int64,067:int64,068:int64,069:int64,070:int64,071:int64,072:int64,"
          "073:int64,074:int64,075:int64,076:int64,077:int64,078:int64,079:int64,080:int64,"
          "081:int64,082:int64,083:int64,084:int64,085:int64,086:int64,087:int64,088:int64,"
          "089:int64,090:int64,091:int64,092:int64,093:int64,094:int64,095:int64,096:int64,"
          "097:int64,098:int64,099:int64,100:int64,101:int64,102:int64,103:int64,104:int64,"
          "105:int64,106:int64,107:int64,108:int64,109:int64,110:int64,111:int64,112:int64,"
          "113:int64,114:int64,115:int64,116:int64,117:int64,118:int64,119:int64,120:int64,"
          "121:int64,122:int64,123:int64,124:int64,125:int64,126:int64,127:int64");
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    ListPtr listptr = boost::any_cast<DataFramePtr>(ddc_read(url,
                                                        "rdataframe",
                                                        conf));
    Rcpp::List list = *(listptr.get());
    EXPECT_EQ(list.size(), 0);
}

TEST_F(DdcTest, ParseSchema) {
    std::map<int32_t, std::pair<std::string,std::string> > refSchema;
    refSchema[0] = std::make_pair("a", "int64");
    refSchema[1] = std::make_pair("b", "character");
    std::map<int32_t, std::pair<std::string,std::string> > schema = parseSchema("a:int64,b:character");
    EXPECT_EQ(refSchema, schema);
}

TEST_F(DdcTest, schema2str) {
    CsvSchema schema;
    schema[0] = std::make_pair("a", "int64");
    schema[1] = std::make_pair("b", "character");
    std::string refSchemaStr = "a:int64,b:character";
    std::string schemaStr = schema2string(schema);
    EXPECT_EQ(schemaStr, refSchemaStr);
}

struct SimpleCsvConfig {
    SimpleCsvConfig(const std::string& _filename,
                    const std::string& _schema,
                    bool _shouldThrow)
        : filename(_filename),
          schema(_schema),
          shouldThrow(_shouldThrow){

    }
    std::string filename;
    std::string schema;
    bool shouldThrow;

    std::string toString() const {
        return str(boost::format("filename: %s, schema: %s, shouldThrow: %d")  %
                   filename % schema % shouldThrow);
    }
};

struct CsvConfig : public SimpleCsvConfig {
    CsvConfig(const std::string& f,
              const std::string& s,
              const std::string& ot,
              const uint64_t nLines,
              const uint64_t nCols) :
        SimpleCsvConfig(f, s, false),  // shouldn't throw
        objectType(ot),
        numLines(nLines),
        numCols(nCols)
    {
    }

    std::string toString() const {
        return str(boost::format("filename: %s, schema: %s, objectType: %s, nLines: %ld nCols: %ld")  %
                   filename % schema % objectType % numLines % numCols);
    }


    std::string objectType;
    const uint64_t numLines;
    const uint64_t numCols;
};

::std::ostream& operator<<(::std::ostream& os, const CsvConfig& CsvConfig) {
  return os << CsvConfig.toString();  // whatever needed to print bar to os
}

class DdcBasicCsvTest : public ::testing::TestWithParam<SimpleCsvConfig> {

};

TEST_P(DdcBasicCsvTest, General) {
    const SimpleCsvConfig& p = GetParam();

    std::string url = p.filename;
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, url, "r");
    base::FileStatus status = file->stat();


    base::ConfigurationMap conf;
    conf["schemaUrl"] = p.schema;
    conf["chunkStart"] = (uint64_t)0;
    conf["chunkEnd"] = (uint64_t)status.length;

    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf")   ;

    if (p.shouldThrow) {
        EXPECT_THROW(boost::any_cast<DataFramePtr>(ddc_read(p.filename,
                                                            "rdataframe",
                                                             conf)),
                     std::runtime_error);
    }
    else {
        ListPtr listptr =
            boost::any_cast<DataFramePtr>(ddc_read(p.filename,
                                               "rdataframe",
                                                conf));
            Rcpp::DataFrame dataFrame = Rcpp::DataFrame(*(listptr.get()));
    }
}

INSTANTIATE_TEST_CASE_P(CsvFiles, DdcBasicCsvTest, ::testing::Values(
    SimpleCsvConfig("../ddc/test/data/ex003.csv",
              "000:int64,001:character",
                    false),
    SimpleCsvConfig("../ddc/test/data/ex004.csv",
              "000:int64,001:character",
                    false),
    SimpleCsvConfig("../ddc/test/data/ex005.csv",
              "000:int64,001:character",
                    false),
    SimpleCsvConfig("../ddc/test/data/ex006.csv",
              "000:int64,001:character",
                    false),
    SimpleCsvConfig("../ddc/test/data/ex007.csv",
              "000:int64,001:character",
                    false),
    SimpleCsvConfig("../ddc/test/data/ex008.csv",  // CSV file with a heading
              "000:int64,001:character",
                    false)
));


/**
 * Partial tests, don't check every record
 */
class DdcSyntheticCsvTest : public ::testing::TestWithParam<CsvConfig> {

};

TEST_P(DdcSyntheticCsvTest, General)
{

    const CsvConfig& p = GetParam();

    std::string url = p.filename;
    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, url, "r");
    base::FileStatus status = file->stat();



  const uint32_t numIterations = 1;
  for(uint64_t i = 0; i < numIterations; i++) {

      //offsets only needed for .offsetcsv files
      std::vector<uint64_t> offsets;
      offsets.reserve(p.numLines);
      for(uint64_t i = 0; i < p.numLines; i++) {
          offsets.push_back(i * p.numCols * 16); //each csv cell has 16 characters in our test data
      }

      base::ConfigurationMap conf;
      conf["schemaUrl"] = p.schema;
      conf["chunkStart"] = (uint64_t)0;
      conf["chunkEnd"] = (uint64_t)status.length;
      conf["offsets"] = offsets;

      conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");

      if(p.objectType == "rdataframe") {

          ListPtr listptr =
                  boost::any_cast<DataFramePtr>(ddc_read(p.filename,
                                                    p.objectType,
                                                    conf));

          Rcpp::DataFrame dataFrame = Rcpp::DataFrame(*(listptr.get()));
          EXPECT_EQ((uint64_t)dataFrame.size(), p.numCols);
          //std::string firstColName = str(boost::format("%03d")  % (0));
          //Rcpp::NumericVector firstCol = dataFrame[firstColName];
          Rcpp::NumericVector firstCol = dataFrame[0];
          //std::string lastColName = str(boost::format("%03d")  % (p.numCols - 1));
          Rcpp::NumericVector lastCol = dataFrame[p.numCols - 1];
          EXPECT_EQ((uint64_t)firstCol.size(), p.numLines);
          EXPECT_EQ((uint64_t)lastCol.size(), p.numLines);
          EXPECT_EQ(firstCol[0],0);
          EXPECT_EQ(firstCol[p.numLines - 1], p.numLines*p.numCols - p.numCols);
          EXPECT_EQ(lastCol[0], p.numCols - 1);
          EXPECT_EQ(lastCol[p.numLines- 1], p.numLines*p.numCols - 1);
      }
      else if(p.objectType == "devnull") {
          boost::shared_ptr<std::vector<boost::any> > dfptr =
                  boost::any_cast<boost::shared_ptr<std::vector<boost::any> > >(ddc_read(p.filename,
                                                                                         p.objectType,
                                                                                         conf));
          EXPECT_EQ((dfptr.get())->size(), p.numCols * p.numLines);
      }
  }
}

INSTANTIATE_TEST_CASE_P(CsvFiles, DdcSyntheticCsvTest, ::testing::Values(

    CsvConfig("../ddc/test/data/test4MB.csv",
              "000:int64,"
              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64",
              "rdataframe",
              16 * 1024,
              16),
    CsvConfig("../ddc/test/data/test4MB.csv",
              "000:int64,"
              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64",
              "devnull",
              16 * 1024,
              16),
    CsvConfig("../ddc/test/data/test4MB.offsetcsv",
              "000:int64,"
              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64",
              "rdataframe",
              16 * 1024,
              16),

    CsvConfig("../ddc/test/data/test4MB.offsetcsv",
              "000:int64,"
              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64",
              "devnull",
              16 * 1024,
              16)
//    CsvConfig("../ddc/test/data/test512MB.csv",
//              "000:int64,"
//              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
//              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64,016:int64,"
//              "017:int64,018:int64,019:int64,020:int64,021:int64,022:int64,023:int64,024:int64,"
//              "025:int64,026:int64,027:int64,028:int64,029:int64,030:int64,031:int64,032:int64,"
//              "033:int64,034:int64,035:int64,036:int64,037:int64,038:int64,039:int64,040:int64,"
//              "041:int64,042:int64,043:int64,044:int64,045:int64,046:int64,047:int64,048:int64,"
//              "049:int64,050:int64,051:int64,052:int64,053:int64,054:int64,055:int64,056:int64,"
//              "057:int64,058:int64,059:int64,060:int64,061:int64,062:int64,063:int64,064:int64,"
//              "065:int64,066:int64,067:int64,068:int64,069:int64,070:int64,071:int64,072:int64,"
//              "073:int64,074:int64,075:int64,076:int64,077:int64,078:int64,079:int64,080:int64,"
//              "081:int64,082:int64,083:int64,084:int64,085:int64,086:int64,087:int64,088:int64,"
//              "089:int64,090:int64,091:int64,092:int64,093:int64,094:int64,095:int64,096:int64,"
//              "097:int64,098:int64,099:int64,100:int64,101:int64,102:int64,103:int64,104:int64,"
//              "105:int64,106:int64,107:int64,108:int64,109:int64,110:int64,111:int64,112:int64,"
//              "113:int64,114:int64,115:int64,116:int64,117:int64,118:int64,119:int64,120:int64,"
//              "121:int64,122:int64,123:int64,124:int64,125:int64,126:int64,127:int64",
//              "rdataframe",
//              256 * 1024,
//              128),
//    CsvConfig("hdfs:///test512MB.csv",
//              "000:int64,"
//              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
//              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64,016:int64,"
//              "017:int64,018:int64,019:int64,020:int64,021:int64,022:int64,023:int64,024:int64,"
//              "025:int64,026:int64,027:int64,028:int64,029:int64,030:int64,031:int64,032:int64,"
//              "033:int64,034:int64,035:int64,036:int64,037:int64,038:int64,039:int64,040:int64,"
//              "041:int64,042:int64,043:int64,044:int64,045:int64,046:int64,047:int64,048:int64,"
//              "049:int64,050:int64,051:int64,052:int64,053:int64,054:int64,055:int64,056:int64,"
//              "057:int64,058:int64,059:int64,060:int64,061:int64,062:int64,063:int64,064:int64,"
//              "065:int64,066:int64,067:int64,068:int64,069:int64,070:int64,071:int64,072:int64,"
//              "073:int64,074:int64,075:int64,076:int64,077:int64,078:int64,079:int64,080:int64,"
//              "081:int64,082:int64,083:int64,084:int64,085:int64,086:int64,087:int64,088:int64,"
//              "089:int64,090:int64,091:int64,092:int64,093:int64,094:int64,095:int64,096:int64,"
//              "097:int64,098:int64,099:int64,100:int64,101:int64,102:int64,103:int64,104:int64,"
//              "105:int64,106:int64,107:int64,108:int64,109:int64,110:int64,111:int64,112:int64,"
//              "113:int64,114:int64,115:int64,116:int64,117:int64,118:int64,119:int64,120:int64,"
//              "121:int64,122:int64,123:int64,124:int64,125:int64,126:int64,127:int64",
//              "rdataframe",
//              256 * 1024,
//              128)

//    CsvConfig("../ddc/test/data/test512MB.offsetcsv",
//              "000:int64,"
//              "001:int64,002:int64,003:int64,004:int64,005:int64,006:int64,007:int64,008:int64,"
//              "009:int64,010:int64,011:int64,012:int64,013:int64,014:int64,015:int64,016:int64,"
//              "017:int64,018:int64,019:int64,020:int64,021:int64,022:int64,023:int64,024:int64,"
//              "025:int64,026:int64,027:int64,028:int64,029:int64,030:int64,031:int64,032:int64,"
//              "033:int64,034:int64,035:int64,036:int64,037:int64,038:int64,039:int64,040:int64,"
//              "041:int64,042:int64,043:int64,044:int64,045:int64,046:int64,047:int64,048:int64,"
//              "049:int64,050:int64,051:int64,052:int64,053:int64,054:int64,055:int64,056:int64,"
//              "057:int64,058:int64,059:int64,060:int64,061:int64,062:int64,063:int64,064:int64,"
//              "065:int64,066:int64,067:int64,068:int64,069:int64,070:int64,071:int64,072:int64,"
//              "073:int64,074:int64,075:int64,076:int64,077:int64,078:int64,079:int64,080:int64,"
//              "081:int64,082:int64,083:int64,084:int64,085:int64,086:int64,087:int64,088:int64,"
//              "089:int64,090:int64,091:int64,092:int64,093:int64,094:int64,095:int64,096:int64,"
//              "097:int64,098:int64,099:int64,100:int64,101:int64,102:int64,103:int64,104:int64,"
//              "105:int64,106:int64,107:int64,108:int64,109:int64,110:int64,111:int64,112:int64,"
//              "113:int64,114:int64,115:int64,116:int64,117:int64,118:int64,119:int64,120:int64,"
//              "121:int64,122:int64,123:int64,124:int64,125:int64,126:int64,127:int64",
//              "devnull",
//              256 * 1024,
//              128)
));

enum OrcFileKind {
    ORC_TIMESTAMP_EXAMPLE,
    ORC_TEST1_EXAMPLE,
    ORC_OTHER_EXAMPLE
};

struct OrcTestConfig{
    OrcTestConfig(const std::string& _filename,
                  const uint64_t _numRecords,
                  const OrcFileKind _orcFileKind) :
        filename(_filename),
        numRecords(_numRecords),
        orcFileKind(_orcFileKind)
    {

    }
    std::string toString() const {
        return str(boost::format("filename: %s, numRecords: %ld, orcFileKind: %d")  %
                   filename % numRecords % orcFileKind);
    }

    std::string filename;
    uint64_t numRecords;
    OrcFileKind orcFileKind;
};

::std::ostream& operator<<(::std::ostream& os, const OrcTestConfig& orcTestConfig) {
    return os << orcTestConfig.toString();  // whatever needed to print bar to os
}


class DdcOrcTest : public ::testing::TestWithParam<OrcTestConfig> {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DdcOrcTest() {
    // You can do set-up work for each test here.
  }

  virtual ~DdcOrcTest() {
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

Rcpp::List getOrcRefList() {
    Rcpp::List col;
    //CharacterVector a(12);
    col.push_back("\"2037-01-01 00:00:00.000999\"");
    col.push_back("\"2003-01-01 00:00:00.000000222\"");
    col.push_back("\"1999-01-01 00:00:00.999999999\"");
    col.push_back("\"1995-01-01 00:00:00.688888888\"");
    col.push_back("\"2002-01-01 00:00:00.1\"");
    col.push_back("\"2010-03-02 00:00:00.000009001\"");
    col.push_back("\"2005-01-01 00:00:00.000002229\"");
    col.push_back("\"2006-01-01 00:00:00.900203003\"");
    col.push_back("\"2003-01-01 00:00:00.800000007\"");
    col.push_back("\"1996-08-02 00:00:00.723100809\"");
    col.push_back("\"1998-11-02 00:00:00.857340643\"");
    col.push_back("\"2008-10-02 00:00:00.0\"");

    Rcpp::List l;
    l["timestamp"] = col;
    return l;
}

bool orcListsAreEqual(Rcpp::List &a, Rcpp::List &b) {

    Rcpp::List a1 = a[0];
    Rcpp::List a2 = b["timestamp"];

    for(int i = 0; i < a1.size(); i++) {
        if(Rcpp::as<std::string>(a1[i]) != Rcpp::as<std::string>(a2[i])) {
            DLOG(INFO) << Rcpp::as<std::string>(a1[i]) << " vs " << Rcpp::as<std::string>(a2[i]);
            return false;
        }
    }
    return true;
}

Rcpp::List getOrcRefList2() {
    Rcpp::List col0;
    col0.push_back(false);
    col0.push_back(true);

    Rcpp::List l;
    l["0"] = col0;

    return l;
}


bool orcListsAreEqual2(Rcpp::List &a, Rcpp::List &b) {

    //List a1 = a["0"];
    DLOG(INFO) << a.size() << " - " << b.size();
    Rcpp::List a1 = a[0]; //TODO what's the name?
    Rcpp::List a2 = b["0"];

    for(int i = 0; i < 1; i++) { //we only check 1 column for now
        if(Rcpp::as<int>(a1[i]) != Rcpp::as<int>(a2[i])) {
            DLOG(INFO) << Rcpp::as<int>(a1[i]) << " vs " << Rcpp::as<int>(a2[i]);
            return false;
        }
    }
    return true;
}


typedef boost::shared_ptr<Rcpp::DataFrame> RcppDataFramePtr;
typedef boost::shared_ptr<Rcpp::List> RcppListPtr;

TEST_P(DdcOrcTest, General)
{
  // Call GetParam() here to get the Row values
    const OrcTestConfig& p = GetParam();

    base::ConfigurationMap conf;


    std::string protocol = base::utils::getProtocol(p.filename);
    std::string filename;
    if(protocol != "") {
        filename = base::utils::removeSubstrs(p.filename, protocol + "://");
    }
    else {
        filename = p.filename;
    }
    orc::ReaderOptions opts;
    std::unique_ptr<orc::Reader> orcReader;
    if(hdfsutils::isHdfs(protocol)) {
        hdfsutils::HdfsInputStream *p = new hdfsutils::HdfsInputStream(filename);
        base::ConfigurationMap hdfsconf;
        hdfsconf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
        hdfsconf["fileStatCache"] = boost::shared_ptr<base::Cache>(new base::Cache());
        p->configure(hdfsconf);
        std::unique_ptr<orc::InputStream> inputStream(p);
        orcReader = orc::createReader(std::move(inputStream), opts);
    }
    else {
        orcReader = orc::createReader(orc::readLocalFile(filename), opts);
    }
    uint64_t numStripes = orcReader->getNumberOfStripes();
    std::vector<uint64_t> selectedStripes;
    for(uint64_t i = 0; i < numStripes; ++i) {
        selectedStripes.push_back(i);
    }
//    std::vector<std::string> selectedStripesStrings;
//    boost::split(selectedStripesStrings, "0", boost::is_any_of(","));
//    std::vector<uint64_t> selectedStripes;
//    for(uint64_t i = 0; i < selectedStripesStrings.size(); ++i) {
//        selectedStripes.push_back((uint64_t)atoll(selectedStripesStrings[i].c_str()));
//    }
    conf["selectedStripes"] = selectedStripes;
    conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");
    Rcpp::List df = *(boost::any_cast<RcppDataFramePtr>(ddc_read(p.filename,
                                                            "rdataframe",
                                                            conf)));

    if (p.orcFileKind == ORC_TIMESTAMP_EXAMPLE) {
        Rcpp::List ref = getOrcRefList();
        EXPECT_TRUE(orcListsAreEqual(df, ref));
    }
    else if (p.orcFileKind == ORC_TEST1_EXAMPLE) {
        Rcpp::List ref = getOrcRefList2();
        EXPECT_TRUE(orcListsAreEqual2(df, ref));
    }
    else if (p.orcFileKind == ORC_OTHER_EXAMPLE) {
        // No checking, just make sure it finished
    }
    Rcpp::List firstcol = df[0];
    uint64_t numRows = firstcol.size();
    uint64_t numCols = df.size();
    EXPECT_EQ(numRows * numCols, p.numRecords);
}



INSTANTIATE_TEST_CASE_P(OrcFiles, DdcOrcTest, ::testing::Values(
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testTimestamp.orc", 12, ORC_TIMESTAMP_EXAMPLE),
    OrcTestConfig("hdfs:///orc_files/TestOrcFile.testTimestamp.orc", 12, ORC_TIMESTAMP_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.test1.orc", 24, ORC_TEST1_EXAMPLE),

    OrcTestConfig("../recordparser/orc/examples/decimal.orc", 6000, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/nulls-at-end-snappy.orc", 7 * 70000, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testMemoryManagementV12.orc", 2500 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testStripeLevelStats.orc",11000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testDate1900.orc",70000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.columnProjection.orc",21000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/version1999.orc",0, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testWithoutIndex.orc", 50000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/demo-12-zlib.orc",1920800 * 9, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.metaData.orc", 12, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testMemoryManagementV11.orc", 2500 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.emptyFile.orc",0, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/demo-11-zlib.orc",1920800 * 9, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testPredicatePushdown.orc", 3500 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testDate2038.orc",212000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/orc_split_elim.orc", 25000 * 5, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testUnionAndTimestamp.orc", 5077 * 3, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testSnappy.orc", 10000 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testTimestamp.orc",12, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/demo-11-none.orc", 1920800 * 9, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/over1k_bloom.orc", 2098*11, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testStringAndBinaryStatistics.orc", 4 * 2, ORC_OTHER_EXAMPLE),
    OrcTestConfig("../recordparser/orc/examples/TestOrcFile.testSeek.orc", 32768 * 12, ORC_OTHER_EXAMPLE)
    //OrcTestConfig("../recordparser/orc/examples/orc-file-11-format.orc", 7500 * 14, ORC_OTHER_EXAMPLE))  // throws in ORC lib
));

void helper(const std::string& url, const uint64_t i) {
    const uint64_t NUM_ROWS = 838861;
    const uint64_t NUM_COLS = 10;
    const uint64_t RECORDS_PER_CHUNK = NUM_ROWS * NUM_COLS;
    //for (uint64_t i = 0; i < 4; ++i) {
        base::ConfigurationMap conf;
        conf["chunkStart"] = (uint64_t)(i)*128*1024*1024;
        conf["chunkEnd"] = (uint64_t)(i+1)*128*1024*1024;
        if (i == 3) {
            conf["chunkEnd"] = 536871040;
        }
        conf["schemaUrl"] = std::string("a0:int64,a1:int64,a2:int64,a3:int64,a4:int64,"
                                        "a5:int64,a6:int64,a7:int64,a8:int64,a9:int64");
        conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");

        Rcpp::List dataFrame =
            *(boost::any_cast<RcppDataFramePtr>(
                ddc_read(url, "rdataframe", conf)));


        Rcpp::NumericVector firstCol = dataFrame[0];
        Rcpp::NumericVector lastCol = dataFrame[NUM_COLS-1];
        EXPECT_EQ((uint64_t)firstCol.size(), NUM_ROWS);
        EXPECT_EQ((uint64_t)lastCol.size(), NUM_ROWS);

        EXPECT_EQ(firstCol[0],0+(i*RECORDS_PER_CHUNK));
        EXPECT_EQ(firstCol[NUM_ROWS - 1], ((NUM_ROWS-1) * NUM_COLS) +(i*RECORDS_PER_CHUNK));
        EXPECT_EQ(lastCol[0], (NUM_COLS - 1) +(i*RECORDS_PER_CHUNK));
        EXPECT_EQ(lastCol[NUM_ROWS - 1], ((NUM_ROWS * NUM_COLS) - 1)+(i*RECORDS_PER_CHUNK));
    //}

}

TEST_F(DdcTest, Local512_10cols_0) {
    helper(std::string("../ddc/test/data/test512MB_10cols.csv"),0);
}
TEST_F(DdcTest, Local512_10cols_1) {
    helper(std::string("../ddc/test/data/test512MB_10cols.csv"),0);
}
TEST_F(DdcTest, Local512_10cols_2) {
    helper(std::string("../ddc/test/data/test512MB_10cols.csv"),0);
}
TEST_F(DdcTest, Local512_10cols_3) {
    helper(std::string("../ddc/test/data/test512MB_10cols.csv"),0);
}


TEST_F(DdcTest, Hdfs512_10cols_0) {
    helper(std::string("hdfs:///test512MB_10cols.csv"),0);
}
TEST_F(DdcTest, Hdfs512_10cols_1) {
    helper(std::string("hdfs:///test512MB_10cols.csv"),1);
}
TEST_F(DdcTest, Hdfs512_10cols_2) {
    helper(std::string("hdfs:///test512MB_10cols.csv"),2);
}
TEST_F(DdcTest, Hdfs512_10cols_3) {
    helper(std::string("hdfs:///test512MB_10cols.csv"),3);
}

void randomizeChunk(const uint64_t start,
                    const uint64_t end,
                    uint64_t *chunkStart,
                    uint64_t *chunkEnd) {
    uint64_t n1 =  0;
    uint64_t n2 =  0;
    do {
        n1 = start + ( std::rand() % ( end - start + 1 ) );
        n2 = start + ( std::rand() % ( end - start + 1 ) );

        if (n1 > n2) {
            *chunkEnd = n1;
            *chunkStart = n2;
        }
        else {
            *chunkEnd = n2;
            *chunkStart = n1;
        }
    } while (n1 == n2);
}

TEST_F(DdcTest, DISABLED_Hdfs512_10cols_randomized) {
    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t chunkStart = 0;
        uint64_t chunkEnd = 0;
        randomizeChunk(0, 536871040, &chunkStart, &chunkEnd);
        base::ConfigurationMap conf;
        conf["chunkStart"] = chunkStart;
        conf["chunkEnd"] = chunkEnd;
        conf["schemaUrl"] = std::string("a0:int64,a1:int64,a2:int64,a3:int64,a4:int64,"
                                        "a5:int64,a6:int64,a7:int64,a8:int64,a9:int64");
        conf["hdfsConfigurationFile"] = std::string("../ddc/test/data/server.conf");

        Rcpp::List dataFrame =
            *(boost::any_cast<RcppDataFramePtr>(
                ddc_read("hdfs:///test512MB_10cols.csv", "rdataframe", conf)));


        const uint64_t numCols = dataFrame.size();
        if (numCols == 0) {
            throw std::runtime_error("0 cols");
        }
        const Rcpp::NumericVector& firstCol = dataFrame[0];
        const uint64_t numRows = firstCol.size();

        uint64_t lastValue = 0;
        for (uint64_t i = 0; i < numRows; ++i) {
            for (uint64_t j = 0; j < numCols; ++j) {
                const Rcpp::NumericVector& col = dataFrame[j];
                if (i == 0 && j == 0) {
                    lastValue = col[i];
                }
                else {
                    if (col[i] != (lastValue + 1)) {
                        std::ostringstream os;
                        os << "Expected " << (lastValue + 1) <<
                              " but got: " << col[i];
                        throw std::runtime_error(os.str());
                    }
                    lastValue = col[i];
                }
            }
        }
    }
}

} // namespace testing
} // namespace ddc
