#include "localblockreader.h"
#include <stdexcept>
#include <glog/logging.h>


namespace ddc{
namespace blockreader {

LocalBlockReader::LocalBlockReader() : blockSize_(0), filename_(""), configured_(false)
{

}

LocalBlockReader::~LocalBlockReader()
{

}

void LocalBlockReader::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(blockSize_, uint64_t, "blocksize");
    GET_PARAMETER(filename_, std::string, "filename");
    GET_PARAMETER(filename_, std::string, "filename");
    f_ = base::ScopedFilePtr(new base::ScopedFile(filename_));
    configured_ = true;
}

BlockPtr LocalBlockReader::next()
{
    throw std::runtime_error("unimplemented");
}


bool LocalBlockReader::hasNext()
{
    throw std::runtime_error("unimplemented");

}

uint64_t LocalBlockReader::blockSize()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return blockSize_;
}

BlockPtr LocalBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes)
{
    if(!configured_) {
        throw std::runtime_error("need to configure");
    }

    if(numBytes == 0) {
        throw std::runtime_error("requested 0 numBytes");
    }

    //DLOG(INFO) << "blockStart: " << blockStart << " numBytes: " << numBytes;
    f_->seek(blockStart);

    boost::shared_ptr<std::vector<uint8_t> > v = boost::shared_ptr<std::vector<uint8_t> >(new std::vector<u_int8_t>);
    v->resize(numBytes);
    size_t bytesRead = f_->read(v->data(), numBytes);
    if(bytesRead != numBytes) {
        //error
        switch(bytesRead) {
            case 0: {
                if(f_->eof()) throw std::runtime_error("eof");
                else throw std::runtime_error("error reading");
                break;
            }
            default: {
                throw std::runtime_error("read less bytes than expected");
            }
        }
    }

    BlockPtr block = BlockPtr(new Block(v));
    return block;
}

} // namespace blockreader
} // namespace ddc

