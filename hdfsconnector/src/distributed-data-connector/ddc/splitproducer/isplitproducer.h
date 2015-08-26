#ifndef DDC_SPLITPRODUCER_ISPLITPRODUCER_H
#define DDC_SPLITPRODUCER_ISPLITPRODUCER_H

#include <boost/any.hpp>
#include <boost/shared_ptr.hpp>
#include "base/configurationmap.h"
#include "base/iiterator.h"
#include "blockreader/iblockreader.h"
#include "splitproducer/split.h"
//#include "splitproducer/splitproducerconfiguration.h"


namespace ddc {
namespace splitproducer {


class EmptySplitException: public std::runtime_error {
public:
    explicit EmptySplitException(const std::string &what): std::runtime_error(what) {

    }
private:
};

/**
 * @brief Produces splits (e.g. a row in CSV files)
 */
class ISplitProducer : public base::IIterator<boost::shared_ptr<Split> > {
public:
    virtual ~ISplitProducer(){

    }

    virtual void configure(base::ConfigurationMap &conf) = 0;

    /**
     * @brief hasNext Returns true if there are more splits
     * @return
     */
    virtual bool hasNext() = 0;

    /**
     * @brief next Returns the next split
     * @return
     */
    virtual boost::shared_ptr<Split> next() = 0;

};

typedef boost::shared_ptr<ISplitProducer> ISplitProducerPtr;

} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_ISPLITPRODUCER_H
