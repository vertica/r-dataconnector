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


#include <string.h> //for memcmp
#include <iostream>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include "blockreader/blockreaderfactory.h"
#include "splitproducer/splitproducerfactory.h"
#include "fakeblockreader.h"

//TODO refactor common code into helper methods

using namespace std;

namespace ddc {
namespace splitproducer {
namespace testing {

struct SplitProducerConfig{
    SplitProducerConfig(const std::string& st,
                        const uint8_t d,
                        const uint64_t ss,
                        const uint64_t se,
                        const uint64_t fe,
                        const std::vector<boost::shared_ptr<Split> >&ref,
                        const std::vector<std::string>& bl,
                        const std::vector<uint64_t>& of,
                        bool should) :
        splitProducerType(st), delimiter(d), splitStart(ss),
        splitEnd(se), fileEnd(fe), refSplits(ref), blocks(bl), offsets(of), shouldThrow(should){

    }
    std::string toString() const {
        return str(boost::format("delimiter: %c splitStart: %ld, splitEnd: %ld, fileEnd: %ld")  %
                   delimiter % splitStart % splitEnd % fileEnd);
    }

    std::string splitProducerType;
    uint8_t delimiter;
    uint64_t splitStart;
    uint64_t splitEnd;
    uint64_t fileEnd;
    std::vector<boost::shared_ptr<Split> > refSplits;
    std::vector<std::string> blocks;
    std::vector<uint64_t> offsets;
    bool shouldThrow;
};

::std::ostream& operator<<(::std::ostream& os, const SplitProducerConfig& ddcConfig) {
  return os << ddcConfig.toString();  // whatever needed to print bar to os
}

// The fixture for testing class Foo.
class SplitProducerTest : public ::testing::TestWithParam<SplitProducerConfig> {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SplitProducerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~SplitProducerTest() {
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


bool areEqual(const boost::shared_ptr<Split>& a, const boost::shared_ptr<Split>& b) {
    if(a->used != b->used) {
        printf("used different %s vs %s\n",  a->buffer, b->buffer);
        return false;
    }
    for(uint64_t i = 0; i < a->used; i++)  {
        if(a->buffer[i] != b->buffer[i]) {
            printf("a: %02x b: %02x | %s vs %s\n", a->buffer[i], b->buffer[i], a->buffer, b->buffer);
            //std::cout << "a: " << a->buffer[i] << " b: " << b->buffer[i] << std::endl;
            return false;
        }
    }
    return true;

    //int res = !(memcmp(a->buffer, b->buffer, a->used));
}

bool areEqual(const std::vector<boost::shared_ptr<Split> >& a, const std::vector<boost::shared_ptr<Split> >& b) {
    if(a.size() != b.size()) {
        DLOG(INFO) << "size different";
        return false;
    }
    for(uint64_t i = 0; i < a.size(); i++) {
        if(!areEqual(a[i],b[i])) {
            DLOG(INFO) << a[i] << " vs " << b[i];
            return false;
        }
    }
    return true;
}

TEST_F(SplitProducerTest, Whateva) {

}

TEST_P(SplitProducerTest, Generic)
{
  // Call GetParam() here to get the Row values
  const SplitProducerConfig& p = GetParam();

  ISplitProducerPtr s = SplitProducerFactory::makeSplitProducer(p.splitProducerType);
  boost::shared_ptr<blockreader::testing::FakeBlockReader> b(new blockreader::testing::FakeBlockReader);
  b->setBlocks(p.blocks);

  base::ConfigurationMap conf;
  conf["delimiter"] = p.delimiter;
  conf["splitStart"] = p.splitStart;
  conf["splitEnd"] = p.splitEnd;
  conf["fileEnd"] = p.fileEnd;
  conf["blockReader"] = (blockreader::IBlockReaderPtr)b;

  conf["offsets"] = p.offsets;

  conf["skipHeader"] = false;

  s->configure(conf);

  std::vector<boost::shared_ptr<Split> > splits;
  bool check = true;
  while(s->hasNext()) {
      try {
        splits.push_back(s->next());
      }
      catch(std::runtime_error& e) {
          EXPECT_TRUE(p.shouldThrow);
          check = false;
      }
  }
    if(check) EXPECT_TRUE(areEqual(p.refSplits, splits));
}

/**
 * 0
 */
std::vector<boost::shared_ptr<Split> > refSplits;
const std::vector<boost::shared_ptr<Split> >& getRefSplits(int index) {
    refSplits.clear();
    if(index == 0) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,aaa,0"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("1,bbb,1"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("2,ccc,2"))));
    }
    else if(index == 1) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,aaa,0"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("1,bbb,1"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("2,ccc,2"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("3"))));

    }
    else if(index == 2) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,a"))));

    }
    else if(index == 3) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,aa"))));

    }
    else if(index == 4) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,aaa"))));
    }
    else if(index == 5) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,1,2,3,4"))));
    }
    else if(index == 6) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,a"))));
    }
    else if(index == 7) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("2,3"))));
    }
    else if(index == 8) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("3,4"))));
    }
    else if(index == 9) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("3,4"))));
    }
    else if(index == 10) {
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("0,1,2,3,4,"))));
        refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("5,6,7,8,9"))));

    }
    else {
        throw std::runtime_error("nope");
    }

    return refSplits;
}
std::vector<std::string> blocks;
const std::vector<std::string>& getBlocks(int index) {
    blocks.clear();
    if(index == 0) {
        blocks.push_back("0,aaa,0\n1,bbb,1\n2,ccc,2");
    }
    else if(index == 1) {
        blocks.push_back("0,aaa,0\n1,bbb,1\n2,ccc,2\n3");
    }
    else if(index == 2) {
        blocks.push_back("0,a");
    }
    else if(index == 3) {
        blocks.push_back("0,aa");
    }
    else if(index == 4) {
        blocks.push_back("0,aaa");
    }
    else if(index == 5) {
        blocks.push_back("0,1,2,3,4\n");
    }
    else if(index == 6) {
        blocks.push_back("0,a\n1,b\n");
    }
    else if(index == 7) {
        blocks.push_back("0,1\n2,3\n");
    }
    else if(index == 8) {
        blocks.push_back("0,1,2\n3,4\n");
    }
    else if(index == 9) {
        blocks.push_back("0,1,2,3,4,5,6,7,8,9,10\n");
    }
    else if(index == 10) {
        blocks.push_back("0,1,2,3,4,5,6,7,8,9");

    }
    else {
        throw std::runtime_error("nope");
    }
    return blocks;
}


std::vector<uint64_t> offsets;
const std::vector<uint64_t>& getOffsets(int index) {
    offsets.clear();
    if(index == 0) {
    }
    else if(index == 1) {
        offsets.push_back(0);offsets.push_back(10);
    }
    else {
        throw std::runtime_error("nope");
    }
    return offsets;
}
INSTANTIATE_TEST_CASE_P(All, SplitProducerTest, ::testing::Values(

////Doesn't work with fake because we need to do ->setSplits()
//    SplitProducerConfig(std::string("fake"),
//                        (uint8_t)'\n',
//                        0,
//                        0,
//                        0,
//                        getRefSplits(0),
//                        std::vector<std::string>(),
//                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                        (uint8_t)'\n',
                        0,
                        getBlocks(0)[0].size(),
                        getBlocks(0)[0].size(),
                        getRefSplits(0),
                        getBlocks(0),
                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                        (uint8_t)'\n',
                        0,
                        getBlocks(1)[0].size(),
                        getBlocks(1)[0].size(),
                        getRefSplits(1),
                        getBlocks(1),
                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                        (uint8_t)'\n',
                        0,
                        getBlocks(2)[0].size(),
                        getBlocks(2)[0].size(),
                        getRefSplits(2),
                        getBlocks(2),
                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                        (uint8_t)'\n',
                        0,
                        getBlocks(3)[0].size(),
                        getBlocks(3)[0].size(),
                        getRefSplits(3),
                        getBlocks(3),
                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                      (uint8_t)'\n',
                      0,
                      getBlocks(4)[0].size(),
                      getBlocks(4)[0].size(),
                      getRefSplits(4),
                      getBlocks(4),
                        getOffsets(0), false),
    SplitProducerConfig(std::string("csv"),
                      (uint8_t)'\n',
                      0,
                      getBlocks(5)[0].size(),
                      getBlocks(5)[0].size(),
                      getRefSplits(5),
                      getBlocks(5),
                        getOffsets(0), false),
SplitProducerConfig(std::string("csv"),
                    (uint8_t)'\n',
                    0,
                    4,
                    getBlocks(6)[0].size(),
                    getRefSplits(6),
                    getBlocks(6),
                    getOffsets(0), false),
SplitProducerConfig(std::string("csv"),
                    (uint8_t)'\n',
                    4,
                    getBlocks(7)[0].size(),
                    getBlocks(7)[0].size(),
                    getRefSplits(7),
                    getBlocks(7),
                    getOffsets(0), false),
SplitProducerConfig(std::string("csv"),
                  (uint8_t)'\n',
                  6,
                  getBlocks(8)[0].size(),
                  getBlocks(8)[0].size(),
                  getRefSplits(8),
                  getBlocks(8),
                  getOffsets(0), false),
SplitProducerConfig(std::string("csv"),
                  (uint8_t)'\n',
                  4,
                  8,
                  getBlocks(9)[0].size(),
                  getRefSplits(9),
                  getBlocks(9),
                  getOffsets(0), true),
SplitProducerConfig(std::string("offsetcsv"),
                  (uint8_t)'\n',
                  0,
                  getBlocks(10)[0].size(),
                  getBlocks(10)[0].size(),
                  getRefSplits(10),
                  getBlocks(10),
                  getOffsets(1), false)
));


TEST_F(SplitProducerTest, RealBlockReader)
{

  ISplitProducerPtr s = SplitProducerFactory::makeSplitProducer("csv");
  blockreader::IBlockReaderPtr b = blockreader::BlockReaderFactory::makeBlockReader("file");


  base::ConfigurationMap blockReaderConf;
  blockReaderConf["blocksize"] = (uint64_t)(32 * 1024 * 1024);
  blockReaderConf["filename"] = std::string("../ddc/test/data/ex001.csv");
  b->configure(blockReaderConf);

  base::ConfigurationMap conf;
  conf["delimiter"] = (uint8_t)'\n';
  conf["splitStart"] = (uint64_t)9;
  conf["splitEnd"] = (uint64_t)18;
  conf["fileEnd"] = (uint64_t)18;
  conf["blockReader"] = b;
  s->configure(conf);

  std::vector<boost::shared_ptr<Split> > splits;
  while(s->hasNext()) {
    splits.push_back(s->next());
  }

  std::vector<boost::shared_ptr<Split> > refSplits;
  refSplits.push_back(boost::shared_ptr<Split>(new Split(boost::make_shared<std::string>("3,ccc"))));

  EXPECT_TRUE(areEqual(splits, refSplits));
}



}//namespace testing
}//namespace splitproducer
}//namespace ddc
