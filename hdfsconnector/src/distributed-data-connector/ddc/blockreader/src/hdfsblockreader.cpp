#include "hdfsblockreader.h"

namespace ddc{
namespace blockreader {

HdfsBlockReader::HdfsBlockReader() :
    blockSize_(0),
    configured_(false),
    fileSize_(0)
{
}

HdfsBlockReader::~HdfsBlockReader()
{

}

void HdfsBlockReader::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(filename_, std::string, "filename");
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");

    hdfsutils::HdfsFile file(filename_);
    base::ConfigurationMap blockLocatorConf;
    blockLocatorConf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    file.configure(blockLocatorConf);
    base::FileStatus s = file.stat();
    blockSize_ = s.blockSize;
    fileSize_ = s.length;
    configured_ = true;
}

BlockPtr HdfsBlockReader::next()
{
    throw std::runtime_error("unimplemented");
}


bool HdfsBlockReader::hasNext()
{
    throw std::runtime_error("unimplemented");

}

BlockPtr HdfsBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes)
{
    if(!configured_) {
        throw std::runtime_error("need to configure");
    }

    if(numBytes == 0) {
        throw std::runtime_error("requested 0 numBytes");
    }

    if (blockStart >= fileSize_) {
        // return empty buffer
        boost::shared_ptr<std::vector<uint8_t> > v = boost::shared_ptr<std::vector<uint8_t> >(new std::vector<u_int8_t>);
        BlockPtr block = BlockPtr(new Block(v));
        return block;
    }

    hdfsutils::HdfsBlockLocator locator;
    base::ConfigurationMap conf;
    conf["filename"] = filename_;
    conf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    locator.configure(conf);
    hdfsutils::BufferPtr buffer = locator.getBlock(blockStart, numBytes);

    BlockPtr block = BlockPtr(new Block(buffer));
    return block;
}

} // namespace blockreader
} // namespace ddc

