#include "fakeassembler.h"
#include <stdexcept>
#include <utility>
#include <boost/shared_ptr.hpp>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <Rcpp.h>
#include "recordparser/recordparserfactory.h"
#include "base/utils.h"

namespace ddc {
namespace assembler {


FakeAssembler::FakeAssembler() : index_(0), configured_(false)
{

}

FakeAssembler::~FakeAssembler()
{

}

void FakeAssembler::configure(base::ConfigurationMap &conf)
{
    recordParser_ = boost::any_cast<recordparser::IRecordParserPtr>(conf["recordParser"]);
    format_ = boost::any_cast<std::string>(conf["format"]);
    if(format_ != "row" && format_ != "column") {
        throw std::runtime_error("format needs to be row or column");
    }
    configured_ = true;

}


static void handleVectors(AnyVector &v, boost::any& value) {
    switch (v.which()) {
        case 0:
        {
            NumericVectorPtr v2 = boost::get<NumericVectorPtr>(v);
            try{
                v2->push_back(boost::any_cast<int64_t>(value));
            }
            catch(boost::bad_any_cast& e) {
                LOG(ERROR) << "error casting int64_t";
            }

            break;
        }
        case 1:
        {
            CharacterVectorPtr v2 = boost::get<CharacterVectorPtr>(v);
            try {
                v2->push_back(boost::any_cast<std::string>(value));
            }
            catch(boost::bad_any_cast& e) {
                LOG(ERROR) << "error casting string";
            }
            break;
        }

        case 2:
        {
            DoubleVectorPtr v2 = boost::get<DoubleVectorPtr>(v);
            try {
                v2->push_back(boost::any_cast<double>(value));
            }
            catch(boost::bad_any_cast& e) {
                LOG(ERROR) << "error casting double";
            }
            break;
        }
        default:
        {
            throw std::runtime_error("Unsupported vector type");
        }
    }
}


boost::any FakeAssembler::getObject()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }

    //set schema to integer, string, date
    std::map<int32_t, AnyVector> columns;
    columns.insert(make_pair(0, NumericVectorPtr(new std::vector<int64_t>())));
    columns.insert(make_pair(1, CharacterVectorPtr(new std::vector<std::string>())));
    columns.insert(make_pair(2, DoubleVectorPtr(new std::vector<double>())));

    recordParser_->registerListener(this);
    while(recordParser_->hasNext()) {
        if((uint64_t)index_ >= columns.size()) {
            throw std::runtime_error("fetching col out of bounds");
        }
        DLOG(INFO) << "fetching value for col " << index_;
        AnyVector v = columns[index_];
        if(format_ == "row") {
            index_ += 1;
        }
        boost::any value = recordParser_->next();
        handleVectors(v, value);

    }


    Rcpp::NumericVector a = Rcpp::wrap(*(boost::get<NumericVectorPtr>(columns[0]).get()));
    Rcpp::CharacterVector b = Rcpp::wrap(*(boost::get<CharacterVectorPtr>(columns[1]).get()));
    Rcpp::DoubleVector c = Rcpp::wrap(*(boost::get<DoubleVectorPtr>(columns[2]).get()));

    Rcpp::DataFrame df = Rcpp::DataFrame::create(
        Rcpp::Named("a")=a,
        Rcpp::Named("b")=b,
        Rcpp::Named("c")=c
    );
    return boost::any(df);

}

void FakeAssembler::update(int32_t level)
{
    if(level == 0) {
        //DLOG(INFO) << "split completed, resetting index!";
        if(format_ == "row") {
            index_ = 0;
        }
        else if(format_ == "column") {
            ++index_;
        }

    }
    else if (level == 1) {
        //DLOG(INFO) << "Initializing new stripe";
        index_ = 0;

    }
    else {
        throw std::runtime_error("unsupported level");
    }
}




}//namespace assembler
}//namespace ddc
