#include "devnullassembler.h"
#include <stdexcept>
#include <utility>
#include <boost/shared_ptr.hpp>
#include <Rcpp.h>
#include "recordparser/recordparserfactory.h"
#include "base/utils.h"

namespace ddc {
namespace assembler {


DevNullAssembler::DevNullAssembler() : index_(0), configured_(false)
{

}

DevNullAssembler::~DevNullAssembler()
{

}

void DevNullAssembler::configure(base::ConfigurationMap &conf)
{
    recordParser_ = boost::any_cast<recordparser::IRecordParserPtr>(conf["recordParser"]);
    configured_ = true;

}




boost::any DevNullAssembler::getObject()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }

    //shared_ptr<recordparser::IRecordParser> recordParser(recordparser::RecordParserFactory::makeRecordParser(base::utils::getExtension(url)));
    recordParser_->registerListener(this);
    boost::shared_ptr<std::vector<boost::any> >values = boost::shared_ptr<std::vector<boost::any> >(new std::vector<boost::any>());
    //values->reserve(512*1024*1024);  // TODO try to reserve vector upfront
    while(recordParser_->hasNext()) {
        boost::any value = recordParser_->next();
        values->push_back(value);

    }
    //Rcpp::DataFrame df;
    return boost::any(values);

}

void DevNullAssembler::update(int32_t level)
{
}




}//namespace assembler
}//namespace ddc
