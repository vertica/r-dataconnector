#ifndef DDC_SPLITPRODUCER_SPLITPRODUCERFACTORY_H
#define DDC_SPLITPRODUCER_SPLITPRODUCERFACTORY_H

#include <string>
#include <boost/shared_ptr.hpp>
#include "isplitproducer.h"

namespace ddc{
namespace splitproducer{

class SplitProducerFactory
{
public:
    SplitProducerFactory();
    ~SplitProducerFactory();

    static ISplitProducerPtr makeSplitProducer(const std::string& fileExtension);
};

} // namespace splitproducer
} // namespace ddc

#endif // DDC_SPLITPRODUCER_SPLITPRODUCERFACTORY_H
