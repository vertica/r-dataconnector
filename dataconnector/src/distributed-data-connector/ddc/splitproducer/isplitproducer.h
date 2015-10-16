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

    virtual base::ConfigurationMap getDebugInfo() = 0;

};

typedef boost::shared_ptr<ISplitProducer> ISplitProducerPtr;

} // namespace splitproducer
} // namespace ddc


#endif // DDC_SPLITPRODUCER_ISPLITPRODUCER_H
