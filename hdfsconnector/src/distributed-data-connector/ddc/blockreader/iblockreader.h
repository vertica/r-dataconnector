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
