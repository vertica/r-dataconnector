#include "fakeblockreader.h"
#include <stdexcept>
#include <glog/logging.h>

namespace ddc{
namespace blockreader {
namespace testing {

FakeBlockReader::FakeBlockReader() : index_(0)
{

}

void FakeBlockReader::setBlocks(const std::vector<std::string>& blocks) {
    blocks_ = blocks;
}

FakeBlockReader::~FakeBlockReader()
{

}

void FakeBlockReader::configure(base::ConfigurationMap &conf)
{

}

BlockPtr FakeBlockReader::next()
{
    if((uint64_t)index_ >= blocks_.size()) {
        throw std::runtime_error("no more elements");
    }
    boost::shared_ptr<std::string> s = boost::shared_ptr<std::string>(new std::string(blocks_[index_]));
    ++index_;
    BlockPtr block = BlockPtr(new Block(s));
    return block;


}

bool FakeBlockReader::hasNext()
{
    return (uint64_t)index_ < blocks_.size();

}

uint64_t FakeBlockReader::blockSize()
{
    // TODO magic
    return 4;
}

BlockPtr FakeBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes)
{
    std::string str = blocks_[0];
    if(blockStart >= str.size()) {
        throw std::runtime_error("start is out of bounds");
    }
    if((blockStart + numBytes) > str.size()) {
        throw std::runtime_error("end is out of bounds");
    }

//    if((blockStart % blockSize()) != 0) {
//        throw std::runtime_error("start not aligned to block size");
//    }
//    if(((blockStart + numBytes) % blockSize()) != 0) {
//        throw std::runtime_error("end not aligned to block size");
//    }

    std::string sub = str.substr(blockStart, numBytes);
    boost::shared_ptr<std::string> s = boost::shared_ptr<std::string>(new std::string(sub));
    DLOG(INFO) << "requested start: " << blockStart << " numbytes: " << numBytes << " returning " << sub;
    BlockPtr block = BlockPtr(new Block(s));

    return block;


}

} // namespace testing
} // namespace blockreader
} // namespace ddc

