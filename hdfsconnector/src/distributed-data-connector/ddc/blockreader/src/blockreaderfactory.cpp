#include "blockreaderfactory.h"

using boost::shared_ptr;
using std::runtime_error;
using std::string;

namespace ddc{
namespace blockreader{

BlockReaderFactory::BlockReaderFactory()
{

}

BlockReaderFactory::~BlockReaderFactory()
{

}



IBlockReaderPtr BlockReaderFactory::makeBlockReader(const std::string& protocol){

    if(protocol == "fake") return IBlockReaderPtr(new testing::FakeBlockReader());
    else if(hdfsutils::isHdfs(protocol)) {
        return IBlockReaderPtr(new HdfsBlockReader());
    }
    else if( protocol == "file")  {
        return IBlockReaderPtr(new LocalBlockReader());
    }
    else { //no protocol, return local reader
        return IBlockReaderPtr(new LocalBlockReader());
    }
}
}//namespace blockreader
}//namespace ddc
