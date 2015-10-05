#ifndef DDC_BLOCKREADER_BLOCKREADERFACTORY_H
#define DDC_BLOCKREADER_BLOCKREADERFACTORY_H

#include <stdlib.h>

#include <stdexcept>
#include <string>

#include <boost/shared_ptr.hpp>

#include "base/utils.h"
#include "hdfsutils/hdfsutils.h"
#include "blockreader/iblockreader.h"
#include "fakeblockreader.h"
#include "localblockreader.h"
#include "hdfsblockreader.h"
#include "prefetchblockreader.h"


namespace ddc{
namespace blockreader{

class BlockReaderFactory
{
public:
    BlockReaderFactory();
    ~BlockReaderFactory();

    static IBlockReaderPtr makeBlockReader(const std::string& protocol);
};
}//namespace blockreader
}//namespace ddc

#endif // DDC_BLOCKREADER_BLOCKREADERFACTORY_H
