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


#include "csvrecordparser.h"
#include <stdlib.h> //for atoi()
#include <iostream>
#include <glog/logging.h>
#include "base/utils.h"
#include "blockreader/block.h"

namespace ddc {
namespace recordparser {

CsvRecordParser::CsvRecordParser() 
    : rowIndex_(0), 
      configured_(false), 
      recordsProduced_(0),
      delimiter_(','),
      commentCharacter_('#'),
      commentLinesDiscarded_(0),
      blankLinesDiscarded_(0),
      chunkStart_(0),
      chunkEnd_(0)
{    
    observer_ = NULL;
}

CsvRecordParser::~CsvRecordParser()
{
}

void CsvRecordParser::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(splitProducer_, splitproducer::ISplitProducerPtr, "splitProducer");
    #define DDC_COMMA ,
    GET_PARAMETER(schema_, std::map<int32_t DDC_COMMA std::pair<std::string DDC_COMMA std::string> > , "schema");
    #undef DDC_COMMA
    GET_PARAMETER(delimiter_, char, "delimiter");
    GET_PARAMETER(commentCharacter_, char, "commentCharacter");

    GET_PARAMETER(chunkStart_, uint64_t, "chunkStart");
    GET_PARAMETER(chunkEnd_, uint64_t, "chunkEnd");
    GET_PARAMETER(url_, std::string, "url");

    configured_ = true;
}

static bool isCommentLine(const std::string& line,
                          const char commentCharacter) {
    uint64_t index = 0;
    // 1. Remove all whitespace
    while (line[index] == ' ') {
        ++index;
    }
    // 2. If the first non-space character is the comment character return true
    return (line[index] == commentCharacter);
}

void CsvRecordParser::dumpDebugInfo() {
    std::string errorLog;
    base::ConfigurationMap conf = splitProducer_->getDebugInfo();

    /**
     * Csv row
     */
    std::ostringstream os;
    for (uint64_t i = 0; i < row_.size(); ++i) {
        os << row_[i] << ", ";
    }
    os << std::endl;
    LOG(ERROR) << "Csv row: " << os.str();
    errorLog += ("Csv row: " + os.str());

    /**
     * Block
     */
    blockreader::BlockPtr block;
    GET_PARAMETER(block, blockreader::BlockPtr, "block");

    std::string filename = std::string("/tmp/") + std::string(basename((char *)url_.c_str())) + "_" +
            base::utils::to_string(chunkStart_) + "_" +
            base::utils::to_string(chunkEnd_) + "_" +
            "_block.txt";
    std::ostringstream os3;
    os3 << "Dumping block to file " << filename;
    LOG(ERROR) << os3.str();
    errorLog += (os3.str() + "\n");
    base::utils::buffer2file(block->buffer, block->used, filename);

    /**
     * split
     */
    std::string split;
    GET_PARAMETER(split, std::string, "split");
    LOG(ERROR) << "Split: " << split;
    errorLog += ("Split: " + split + "\n");

    /**
     * requested blocks
     */
    std::vector<std::pair<uint64_t,uint64_t> > requestedBlocks;
    #define DDC_COMMA ,
    GET_PARAMETER(requestedBlocks, std::vector<std::pair<uint64_t DDC_COMMA uint64_t> >, "requestedBlocks");
    #undef DDC_COMMA
    std::ostringstream os2;
    os2 << "Requested blocks: " << std::endl;
    for (uint64_t i = 0; i < requestedBlocks.size(); ++i) {
        os2 << "\t" << requestedBlocks[i].first << ", " << requestedBlocks[i].second;
    }
    os2 << std::endl;
    LOG(ERROR) << os2.str();
    errorLog += os2.str();
    std::string filename2 = std::string("/tmp/") + std::string(basename((char *)url_.c_str())) + "_" +
            base::utils::to_string(chunkStart_) + "_" +
            base::utils::to_string(chunkEnd_) + "_" +
            "_errorlog.txt";
    base::utils::buffer2file((uint8_t *)errorLog.data(), errorLog.size(), filename2);

}

boost::any CsvRecordParser::next(){
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    //if we already have a line, consume that
    if((uint64_t)rowIndex_ < row_.size()) {
        if (schema_.find(rowIndex_) == schema_.end() ) {
            dumpDebugInfo();
            std::ostringstream os;
            os << "Invalid schema. Unable to find type for column " << rowIndex_ << ".";
            throw std::runtime_error(os.str());
        }

        boost::any myany;
        bool isNull = false;
        if (row_[rowIndex_] == "") {
            isNull = true;
            myany = CsvRecord(isNull);
        }
        else if (schema_[rowIndex_].second == "logical") {
            myany = CsvRecord(isNull, static_cast<bool>(atoll(row_[rowIndex_].c_str())));
        }
        else if (schema_[rowIndex_].second == "integer") {
            myany = CsvRecord(isNull, (int32_t)atoi(row_[rowIndex_].c_str()));
        }
        else if (schema_[rowIndex_].second == "int64") {
            myany = CsvRecord(isNull, (double)atof(row_[rowIndex_].c_str()));
        }
        else if (schema_[rowIndex_].second == "numeric") {
            myany = CsvRecord(isNull, (double)atof(row_[rowIndex_].c_str()));
        }
        else if (schema_[rowIndex_].second == "character") {
            myany = CsvRecord(isNull, row_[rowIndex_]);
        }        
        else {
            std::ostringstream os;
            os << "Unsupported type ";
            os << schema_[rowIndex_].second;
            os << ". Supported types are int64, double and string";
            throw std::runtime_error(os.str());
        }
        DLOG_IF(INFO, recordsProduced_ < 10) << "returning record: " << row_[rowIndex_];
        ++recordsProduced_;
        rowIndex_++;
        return myany;
    }
    else if((uint64_t)rowIndex_ == row_.size()) {
        if(rowIndex_ != 0) {
            //notify we're done with line
            if(!observer_) {
                throw std::runtime_error("observer not set");
            }
            observer_->update(0);
        }
        if(splitProducer_->hasNext()) {
            //load new line and reset rowIndex_
            boost::shared_ptr<splitproducer::Split> s;
            std::string line;

            bool isCommentLineFlag = false;
            bool isEmptyLineFlag = false;
            do {
                try {
                    s = splitProducer_->next();
                    line = std::string(const_cast<const char*>(reinterpret_cast<char *>(s->buffer)), s->used);
                }
                catch(const splitproducer::EmptySplitException& e) {
                    DLOG(INFO) << "Split producer returned an empty split";
                    throw;
                }
                /**
                 * Discard comment lines
                 */
                isCommentLineFlag = isCommentLine(line, commentCharacter_);
                if (isCommentLineFlag) {
                    ++commentLinesDiscarded_;
                }
                /**
                 * Discard blank lines
                 */
                isEmptyLineFlag = line == "";
                if (isEmptyLineFlag) {
                    ++blankLinesDiscarded_;
                }
            }
            while (isCommentLineFlag || isEmptyLineFlag);

            std::istringstream ss(line);
            text::csv::csv_istream is(ss, delimiter_);
            is >> row_;
            rowIndex_ = 0;
            return next();
        }
        else{
            throw std::runtime_error("no more records");
        }
    }    
}

bool CsvRecordParser::hasNext()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return ((uint64_t)rowIndex_ < row_.size()) || (splitProducer_->hasNext());

}

base::ConfigurationMap CsvRecordParser::getDebugInfo() {
    base::ConfigurationMap conf;
    conf["csvRow"] = row_;
    base::ConfigurationMap spConf = splitProducer_->getDebugInfo();
    conf["block"] = spConf["block"];
    conf["split"] = spConf["split"];
    conf["requestedBlocks"] = spConf["requestedBlocks"];
    return conf;
}


} // namespace recordparsing
} // namespace ddc
