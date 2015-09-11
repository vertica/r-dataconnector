#include "fakeassembler.h"
#include <stdexcept>
#include <utility>
#include <boost/shared_ptr.hpp>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <Rcpp.h>
#include "recordparser/csvrecordparser.h"
#include "recordparser/recordparserfactory.h"
#include "base/utils.h"

namespace ddc {
namespace assembler {

typedef boost::variant<BoolVectorPtr,
                       Int32VectorPtr,
                       DoubleVectorPtr,
                       CharacterVectorPtr,
                       SEXP> AnyVector;


FakeAssembler::FakeAssembler() : index_(0), configured_(false)
{

}

FakeAssembler::~FakeAssembler()
{

}

void FakeAssembler::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(recordParser_, recordparser::IRecordParserPtr, "recordParser");
    GET_PARAMETER(format_, std::string, "format");
    if(format_ != "row" && format_ != "column") {
        throw std::runtime_error("format needs to be row or column");
    }
    configured_ = true;

}


static void handleVectors(AnyVector &v, boost::any& value) {
    switch (v.which()) {
        case 0:
        {
            // bool
            BoolVectorPtr v2 = boost::get<BoolVectorPtr>(v);
            bool v = boost::any_cast<bool>(value);
            v2->push_back(v);
            break;
        }
        case 1:
        {
            // int32
            Int32VectorPtr v2 = boost::get<Int32VectorPtr>(v);
            int32_t v = boost::any_cast<int32_t>(value);
            v2->push_back(v);
            break;
        }
        case 2:
        {
            // double
            DoubleVectorPtr v2 = boost::get<DoubleVectorPtr>(v);
            double v = boost::any_cast<double>(value);
            v2->push_back(v);
            break;
        }
        case 3:
        {
            // string
            CharacterVectorPtr v2 = boost::get<CharacterVectorPtr>(v);
            std::string v = boost::any_cast<std::string>(value);
            v2->push_back(v);
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
    columns.insert(make_pair(0, Int32VectorPtr(new std::vector<int32_t>())));
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


    Rcpp::IntegerVector a = Rcpp::wrap(*(boost::get<Int32VectorPtr>(columns[0]).get()));
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
