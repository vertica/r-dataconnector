#include "hdfsblockreader.h"

namespace ddc{
namespace blockreader {

HdfsBlockReader::HdfsBlockReader() :
    blockSize_(0),
    configured_(false)
{
}

HdfsBlockReader::~HdfsBlockReader()
{

}

void HdfsBlockReader::configure(base::ConfigurationMap &conf)
{
    filename_ = boost::any_cast<std::string>(conf["filename"]);
    hdfsConfigurationFile_ = boost::any_cast<std::string>(conf["hdfsConfigurationFile"]);

    hdfsutils::HdfsFile file(filename_);
    base::ConfigurationMap blockLocatorConf;
    blockLocatorConf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    file.configure(blockLocatorConf);
    base::FileStatus s = file.stat();
    blockSize_ = s.blockSize;

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

