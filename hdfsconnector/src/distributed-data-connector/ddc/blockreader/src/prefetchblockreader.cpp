
#include "prefetchblockreader.h"

namespace ddc {
namespace blockreader {

PrefetchBlockReader::PrefetchBlockReader() :
    blockSize_(0),
    prefetchQueueSize_(2),
    fileSize_(0)
{

}

PrefetchBlockReader::~PrefetchBlockReader() {
    // shutdown worker and join it
    base::Block<Range> *shutdownBlock;
    requestQueue_->getWriteSlot(&shutdownBlock);
    shutdownBlock->shutdownStage = true;
    requestQueue_->slotWritten(shutdownBlock);

    worker_->join();
}

void PrefetchBlockReader::configure(base::ConfigurationMap &conf) {
    GET_PARAMETER(blockSize_, uint64_t, "blocksize");
    GET_PARAMETER(filename_, std::string, "filename");

    try {
        GET_PARAMETER(prefetchQueueSize_, uint64_t, "prefetchQueueSize");
    }
    catch(...) {
        // PASS
    }

    // fetch block reader from factory
    std::string url;
    GET_PARAMETER(url, std::string, "filename"); // parameter comes in url

    std::string protocol = base::utils::getProtocol(url);
    std::string filename = std::string(base::utils::stripProtocol(url));
    if (hdfsutils::isHdfs(protocol)) {
        std::string hdfsConfigurationFile;
        GET_PARAMETER(hdfsConfigurationFile, std::string, "hdfsConfigurationFile");
        hdfsutils::HdfsFile file(filename);
        base::ConfigurationMap blockLocatorConf;
        blockLocatorConf["hdfsConfigurationFile"] = hdfsConfigurationFile;
        file.configure(blockLocatorConf);
        base::FileStatus s = file.stat();
        fileSize_ = s.length;
    }
    else if (protocol == "sleep") {
        //PASS
    }
    else {
        base::ScopedFilePtr f = base::ScopedFilePtr(new base::ScopedFile(filename));
        fileSize_ = f->stat().length;;
    }

    requestQueue_ = boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >(new base::ProducerConsumerQueue<base::Block<Range> > ());
    responseQueue_ = boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > >(new base::ProducerConsumerQueue<base::Block<Range> > ());
    requestQueue_->configure(prefetchQueueSize_);
    responseQueue_->configure(prefetchQueueSize_);


    worker_ = boost::shared_ptr<Worker>(new Worker(requestQueue_,responseQueue_,conf));
    worker_->start();


    configured_ = true;
}

boost::shared_ptr<Block> PrefetchBlockReader::next() {
    throw std::runtime_error("unimplemented");
}

bool PrefetchBlockReader::hasNext() {
    throw std::runtime_error("unimplemented");
}

uint64_t PrefetchBlockReader::blockSize() {
    if(!configured_) {
        throw std::runtime_error("not configured");
    }
    return worker_->blockSize();
}

uint64_t PrefetchBlockReader::requestBlocks(const uint64_t blockStart, const uint64_t numBytes) {



    uint64_t offset = blockStart;
    uint64_t actualNumBytes = numBytes;
    base::Block<Range> *request;

    uint64_t requestedBlocks = 0;

    for (uint64_t i = 0; i < prefetchQueueSize_; ++i) {


        if (fileSize_ == 0) {
            actualNumBytes = numBytes;
        }
        else {
            uint64_t diff = fileSize_ - offset;
            actualNumBytes = std::min(diff, numBytes);
        }

        if (actualNumBytes == 0) return requestedBlocks;

        if (requestedBlocks_.find(Range(offset, actualNumBytes)) != requestedBlocks_.end()) {
            // if we already have a request in flight for this block return
//            DLOG(INFO) << "Not requesting " << offset << ", " << actualNumBytes <<
//                          " because it's on the list";
            continue;
        }

        if (requestQueue_->tryGetWriteSlot(&request)) {
            ++requestedBlocks;

            request->data.offset = offset;
            request->data.numBytes = actualNumBytes;
            DLOG(INFO) << "Requesting block. Offset: " << offset <<
                          " numbytes: " << actualNumBytes;
            requestQueue_->slotWritten(request);

            requestedBlocks_[Range(offset,actualNumBytes)] = true;

            offset += actualNumBytes;
        }
    }
    return requestedBlocks;
}

BlockPtr PrefetchBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes) {
    base::Block<Range> *block;
    BlockPtr res;

    /**
      * Check if the block is present already
      */
    while (responseQueue_->tryGetReadSlot(&block)) {
        if (block->data.offset == blockStart && block->data.numBytes == numBytes) {
            res = block->data.data;


            responseQueue_->slotRead(block);

            // delete from inflight list
            requestedBlocks_.erase(Range(block->data.offset, block->data.numBytes));


            // prefetch next blocks
            uint64_t requestedBlocks = requestBlocks(blockStart + numBytes, numBytes);
            DLOG(INFO) << "1. From cache returning block " << block->data.offset <<
                          ", " << block->data.numBytes;


            return res;
        }
        else {
            /**
              * If there're blocks but not the ones we're looking for, discard them
              */
            DLOG(INFO) << "2. Wrong block ,discarding ... Expected: " << blockStart <<
                          ", " << numBytes << " but got " << block->data.offset <<
                          ", " << block->data.numBytes;
                          ;

            responseQueue_->slotRead(block); // discard this block

            // delete from inflight list
            requestedBlocks_.erase(Range(block->data.offset, block->data.numBytes));

        }
    }

    while (1) {
        /**
         * The block wasn't present. Request it and subsequent ones.
         */
        uint64_t requestedBlocks = requestBlocks(blockStart,numBytes);

        if (responseQueue_->tryGetReadSlot(&block)) {
            if (block->data.offset == blockStart && block->data.numBytes == numBytes) {
                res = block->data.data;

                responseQueue_->slotRead(block);

                // delete from inflight list
                requestedBlocks_.erase(Range(block->data.offset, block->data.numBytes));

                // prefetch next blocks
                requestBlocks(blockStart + numBytes, numBytes);
                DLOG(INFO) << "1. After requesting returning block " << block->data.offset <<
                              ", " << block->data.numBytes;

                return res;
            }
            else {
                /**
                  * Discard wrong blocks if any
                  */
                DLOG(INFO) << "2. Wrong block ,discarding ... Expected: " << blockStart <<
                              ", " << numBytes << " but got " << block->data.offset <<
                              ", " << block->data.numBytes;
                              ;
                responseQueue_->slotRead(block); // discard this block

                // delete from inflight list
                requestedBlocks_.erase(Range(block->data.offset, block->data.numBytes));
            }
        }
    }
    throw std::runtime_error("Unknown error in getBlock");
}

/**
 * Worker
 */

PrefetchBlockReader::Worker::Worker(boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > &requestQueue, boost::shared_ptr<base::ProducerConsumerQueue<base::Block<Range> > > &responseQueue, base::ConfigurationMap &conf) :
    requestQueue_(requestQueue),
    responseQueue_(responseQueue){

    // fetch block reader from factory
    std::string filename;
    GET_PARAMETER(filename, std::string, "filename");
    std::string protocol = base::utils::getProtocol(filename);
    if (hdfsutils::isHdfs(protocol)) {
        blockReader_ = IBlockReaderPtr(new HdfsBlockReader());
    }
    else if (protocol == "sleep") {
        blockReader_ = IBlockReaderPtr(new SleepBlockReader());
    }
    else {
        blockReader_ = IBlockReaderPtr(new LocalBlockReader());
    }
    conf["filename"] = std::string(base::utils::stripProtocol(filename));
    blockReader_->configure(conf);
}

void PrefetchBlockReader::Worker::start() {
    t_ = boost::thread(&Worker::run, this);
}

void PrefetchBlockReader::Worker::join() {
    t_.join();
}

void PrefetchBlockReader::Worker::run() {
    DLOG(INFO) << "Worker starting ...";
    while (1) {
        base::Block<Range> *request;
        requestQueue_->getReadSlot(&request);

        if (request->shutdownStage) {
            LOG(INFO) << "shutting down worker";
            requestQueue_->slotRead(request);
            break;
        }

        uint64_t offset = request->data.offset;
        uint64_t numBytes = request->data.numBytes;
        requestQueue_->slotRead(request);

        // post response
        base::Block<Range> *response;
        responseQueue_->getWriteSlot(&response);
        response->data.offset = offset;
        response->data.numBytes = numBytes;
        response->data.data = blockReader_->getBlock(offset, numBytes);
        DLOG(INFO) << "Response: " << response->data.offset << ", " << response->data.numBytes;
        responseQueue_->slotWritten(response);

    }
    DLOG(INFO) << "Worker exiting ...";
}

uint64_t PrefetchBlockReader::Worker::blockSize() {
    return blockReader_->blockSize();
}

/**
 * SleepBlockReader
 */

PrefetchBlockReader::Worker::SleepBlockReader::SleepBlockReader() : seconds_(0){

}

PrefetchBlockReader::Worker::SleepBlockReader::~SleepBlockReader() {

}

void PrefetchBlockReader::Worker::SleepBlockReader::configure(base::ConfigurationMap &conf) {
    GET_PARAMETER(seconds_, uint64_t, "sleepSeconds");

}

BlockPtr PrefetchBlockReader::Worker::SleepBlockReader::next()
{
    throw std::runtime_error("unimplemented");
}

bool PrefetchBlockReader::Worker::SleepBlockReader::hasNext()
{
    throw std::runtime_error("unimplemented");

}

BlockPtr PrefetchBlockReader::Worker::SleepBlockReader::getBlock(const uint64_t blockStart, const uint64_t numBytes) {
    if (seconds_ == 0) {
        return BlockPtr(new Block());
    }
    boost::posix_time::seconds workTime(seconds_);
    boost::this_thread::sleep(workTime);
    return BlockPtr(new Block());
}

}  // namespace blockreader
}  // namespace ddc
