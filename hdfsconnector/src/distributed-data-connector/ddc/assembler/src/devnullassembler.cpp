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


#include "devnullassembler.h"
#include <stdexcept>
#include <utility>
#include <boost/shared_ptr.hpp>
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
    GET_PARAMETER(recordParser_, recordparser::IRecordParserPtr, "recordParser");
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
    return boost::any(values);

}

void DevNullAssembler::update(int32_t level)
{
}




}//namespace assembler
}//namespace ddc
