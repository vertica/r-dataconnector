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


#ifndef DDC_BLOCKREADER_IBLOCKREADER_H
#define DDC_BLOCKREADER_IBLOCKREADER_H

#include <boost/any.hpp>
#include <boost/shared_ptr.hpp>
#include "base/configurationmap.h"
#include "base/iiterator.h"
#include "blockreader/block.h"

namespace ddc {
namespace blockreader {

/**
 * @brief The IBlockReader class
 */
class IBlockReader : public base::IIterator<boost::shared_ptr<Block> > {
public:
    virtual ~IBlockReader(){
    }
    virtual void configure(base::ConfigurationMap &conf) = 0;

    // not sure if the iterator pattern makes sense here unless the user
    // wants to read the whole file and doesn't care about block size
    virtual bool hasNext() = 0;
    virtual boost::shared_ptr<Block> next() = 0;

    /**
     * @brief blockSize Returns the size of the blocks in bytes.
     * @return
     */
    virtual uint64_t blockSize() = 0;

    /**
     * @brief getBlock Returns a block determined by blockStart and numBytes
     * @param blockStart
     * @param numBytes
     * @return
     */
    virtual BlockPtr getBlock(const uint64_t blockStart, const uint64_t numBytes) = 0;
};

typedef boost::shared_ptr<IBlockReader> IBlockReaderPtr;

} // namespace blockreader
} // namespace ddc


#endif // DDC_BLOCKREADER_IBLOCKREADER_H
