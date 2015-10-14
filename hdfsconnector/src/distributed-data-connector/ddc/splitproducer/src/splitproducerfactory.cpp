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


#include "splitproducerfactory.h"
#include <stdexcept>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include "base/utils.h"
#include "delimitersplitproducer.h"
#include "fakesplitproducer.h"
#include "offsetsplitproducer.h"

using std::runtime_error;
using std::string;

namespace ddc{
namespace splitproducer{

SplitProducerFactory::SplitProducerFactory()
{

}

SplitProducerFactory::~SplitProducerFactory()
{

}



ISplitProducerPtr SplitProducerFactory::makeSplitProducer(const std::string &fileExtension){

    if(fileExtension == "fake") return ISplitProducerPtr(new testing::FakeSplitProducer());
    else if(fileExtension == "csv") return ISplitProducerPtr(new DelimiterSplitProducer());
    else if(fileExtension == "offsetcsv") return ISplitProducerPtr(new OffsetSplitProducer());
    else if(fileExtension == "orc") return ISplitProducerPtr(new testing::FakeSplitProducer()); //TODO ORC doesn't need a splitproducer
    else throw runtime_error(string("No splitproducer class found for file extension ") + fileExtension);
}
}//namespace splitproducer
}//namespace ddc
