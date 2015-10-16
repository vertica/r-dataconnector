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


#ifndef DDC_RECORDPARSER_IRECORDPARSER_H
#define DDC_RECORDPARSER_IRECORDPARSER_H

#include <boost/any.hpp>
#include <boost/shared_ptr.hpp>
#include "base/configurationmap.h"
#include "base/iiterator.h"
#include "base/iobserver.h"
#include "splitproducer/isplitproducer.h"

namespace ddc {
namespace recordparser {

class IRecordParser : public base::IIterator<boost::any> {
public:
    virtual ~IRecordParser(){
    }

    virtual void configure(base::ConfigurationMap &conf) = 0;
    /**
     * @brief hasNext Returns true is there are more records.
     * @return
     */
    virtual bool hasNext() = 0;
    /**
     * @brief next Returns next record.
     * @return
     */
    virtual boost::any next() = 0;

    /**
     * @brief registerListener Register a listener (usually an assembler). Used
     *        to notify when a section of the file (row, column, stripe) is
     *        consumed.
     * @param observer
     */
    virtual void registerListener(base::IObserver<int32_t> *observer)  {
        observer_ = observer;
    }

    virtual base::ConfigurationMap getDebugInfo() = 0;

protected:
    base::IObserver<int32_t> *observer_;
};

typedef boost::shared_ptr<IRecordParser> IRecordParserPtr;


} //namespace recordparser
} //namespace ddc

#endif// DDC_RECORDPARSER_IRECORDPARSER_H
