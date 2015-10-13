
#include "hdfsinputstream.h"

namespace ddc {
namespace hdfsutils {

HdfsInputStream::HdfsInputStream(const std::string &url)
    : url_(url),
      configured_(false)
{
    hdfsFile_ = hdfsutils::HdfsFilePtr(new hdfsutils::HdfsFile(url));

}

HdfsInputStream::~HdfsInputStream()
{

}

void HdfsInputStream::configure(base::ConfigurationMap &conf) {
    base::ConfigurationMap hdfsconf;
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");
    GET_PARAMETER(fileStatCache_, boost::shared_ptr<base::Cache>, "fileStatCache");

    hdfsconf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    hdfsconf["fileStatCache"] = fileStatCache_;
    hdfsFile_->configure(hdfsconf);
    stat_ = hdfsFile_->stat();
    configured_ = true;
}

uint64_t HdfsInputStream::getLength() const
{
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    return stat_.length;
}

class HeapBuffer: public orc::Buffer {
private:
    char* start;
    uint64_t length;

public:
    explicit HeapBuffer(uint64_t size) {
        start = new char[size];
        length = size;
    }


  char *getStart() const {
      return start;
  }

  uint64_t getLength() const {
      return length;
  }
};

orc::Buffer *HdfsInputStream::read(uint64_t offset, uint64_t length, orc::Buffer *buffer)
{
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    if (buffer == NULL) {
      buffer = new HeapBuffer(length);
    } else if (buffer->getLength() < length) {
      delete buffer;
      buffer = new HeapBuffer(length);
    }

    //blockreader::BlockPtr block = hdfsBlockReader_.getBlock(offset, length);

    hdfsutils::HdfsBlockLocator locator;
    base::ConfigurationMap conf;
    conf["filename"] = url_;
    conf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    conf["fileStatCache"] = fileStatCache_;
    locator.configure(conf);
    BufferPtr block = locator.getBlock(offset, length);
    if(block->size() != length) {
        throw std::runtime_error("size mismatch");
    }
    // TODO get rid of this copy
    memcpy(buffer->getStart(), block->data(), block->size());
    return buffer;
}

const std::string &HdfsInputStream::getName() const
{
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    return url_;
}

}  // namespace hdfsutils
}  // namespace ddc
