#include "csvrecordparser.h"
#include <stdlib.h> //for atoi()
#include <iostream>
#include <glog/logging.h>

namespace ddc {
namespace recordparser {

CsvRecordParser::CsvRecordParser() 
    : rowIndex_(0), 
      configured_(false), 
      recordsProduced_(0),
      delimiter_(','),
      commentCharacter_('#'),
      commentLinesDiscarded_(0)
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

boost::any CsvRecordParser::next(){
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    //if we already have a line, consume that
    if((uint64_t)rowIndex_ < row_.size()) {
        boost::any myany;
        if(schema_[rowIndex_].second == "int64") {
            if (row_[rowIndex_] == "") {
                // otherwise atoll converts "" to 0
                myany = row_[rowIndex_];
            }
            else {
                myany = (int64_t)atoll(row_[rowIndex_].c_str());
            }
        }
        else if(schema_[rowIndex_].second == "string") {
                myany = row_[rowIndex_];
        }
        else if(schema_[rowIndex_].second == "double") {
            myany = (double)atof(row_[rowIndex_].c_str());
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
            }
            while (isCommentLineFlag);

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


} // namespace recordparsing
} // namespace ddc
