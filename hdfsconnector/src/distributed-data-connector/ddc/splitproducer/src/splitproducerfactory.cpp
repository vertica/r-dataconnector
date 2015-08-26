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
