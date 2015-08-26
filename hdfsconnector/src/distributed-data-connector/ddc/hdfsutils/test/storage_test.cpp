#include "storage.h"

#include <unistd.h>

#include <gtest/gtest.h>
#include <boost/thread.hpp>

// The fixture for testing class Foo.
class StorageTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StorageTest() {
    // You can do set-up work for each test here.
  }

  virtual ~StorageTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};



class Worker{
public:
    void start(){
        thread_ = thread(&Worker::run, this);
    }
    void join(){
        thread_.join();
    }

    void run() = 0;

protected:
    thread thread_;

};

class Reader: public Worker{
public:
    explicit Reader(Storage<Block<int32_t> > *storage) : storage_(storage){

    }

    void run(){
        Block<int32_t> *block;
        storage_->getReadSlot(&block);
        storage_->slotRead(block);
    }
private:
    Storage<Block<int32_t> > *storage_;
};

class Writer: public Worker{
public:
    explicit Writer(Storage<Block<int32_t> > *storage) : storage_(storage){

    }

    void run(){
        Block<int32_t> *block;
        storage_->getWriteSlot(&block);
        storage_->slotWritten(block);
    }
private:
    Storage<Block<int32_t> > *storage_;
};



TEST_F(StorageTest, Basic) {
    Storage<Block<int32_t> > storage;
    const uint32_t NUM_BUFFERS = 2;
    storage.configure(NUM_BUFFERS);
    Reader r(&storage);
    Writer w(&storage);
    r.start();
    w.start();
    r.join();
    w.join();
}

class Reader2: public Worker{
public:
    explicit Reader2(Storage<Block<int32_t> > *storage) : storage_(storage){

    }

    void run(){
        Block<int32_t> *block;
        int id = 1;
        storage_->getReadSlot(&block, id);
        storage_->slotRead(block);
        int id2 = 0;
        storage_->getReadSlot(&block, id2);
        storage_->slotRead(block);
    }
private:
    Storage<Block<int32_t> > *storage_;
};

class Writer2: public Worker{
public:
    explicit Writer2(Storage<Block<int32_t> > *storage) : storage_(storage){

    }

    void run(){
        printf("adding buf 1\n");
        Block<int32_t> *block;
        storage_->getWriteSlot(&block);
        block->id = 0;
        storage_->slotWritten(block);

        printf("Sleeping ...\n");
        sleep(2);

        printf("adding buf 2\n");
        Block<int32_t> *block2;
        storage_->getWriteSlot(&block2);
        block2->id = 1;
        storage_->slotWritten(block2);

        printf("Done\n");
    }
private:
    Storage<Block<int32_t> > *storage_;
};

TEST_F(StorageTest, WithIds) {
    Storage<Block<int32_t> > storage;
    const uint32_t NUM_BUFFERS = 2;
    storage.configure(NUM_BUFFERS);

    Reader2 r(&storage);
    Writer2 w(&storage);
    r.start();
    w.start();

    r.join();
    w.join();
}

struct Buffer {
    Buffer()
        : buf(NULL),
          size(0),
          used(0) {

    }

    uint8_t *buf;
    uint64_t size;
    uint64_t used;
};

struct Chunk {
    Chunk()
        : offset(0) {

    }

    uint64_t offset;
    Buffer buffer;
};

/*
 *  Download Stage => |111|222|333|
 *  Parse Stage    =>     |111|222|333|
 */
class Downloader: public Worker{
public:
    Downloader(Storage<Block<uint64_t> > *prefetchQueue, Storage<Block<Chunk> > *chunkQueue) :
    prefetchQueue_(prefetchQueue), chunkQueue_(chunkQueue){

    }

    void run(){
        for(int i = 0; i <10; i++) {
            Block<uint64_t> *prefetchBlock;
            //get prefetch order from queue
            prefetchQueue_->getReadSlot(&prefetchBlock);
            uint64_t offset = prefetchBlock->data;
            prefetchQueue_->slotRead(prefetchBlock);

            Block<Chunk> *chunkBlock;
            chunkQueue_->getWriteSlot(&chunkBlock);
            printf("Downloading, offset: %ld\n", offset);
            sleep(1); //downloading time
            chunkBlock->data.offset = offset;
            chunkQueue_->slotWritten(chunkBlock);
        }
    }
private:
    Storage<Block<uint64_t> > *prefetchQueue_;
    Storage<Block<Chunk> > *chunkQueue_;

};

TEST_F(StorageTest, Pipeline) {

    Storage<Block<uint64_t> > prefetchQueue;
    prefetchQueue.configure(2);
    Storage<Block<Chunk> > chunkQueue;
    chunkQueue.configure(2);

    Downloader d(&prefetchQueue,&chunkQueue);
    d.start();

    uint64_t offset = 0;

    //prefetch first block
    Block<uint64_t> *prefetchBlock;
    prefetchQueue.getWriteSlot(&prefetchBlock);
    prefetchBlock->data = offset;
    prefetchQueue.slotWritten(prefetchBlock);
    offset += 1024;

    for(int i = 0; i <10; i++) {

        //prefetch another block
        Block<uint64_t> *prefetchBlock;
        prefetchQueue.getWriteSlot(&prefetchBlock);
        prefetchBlock->data = offset;
        prefetchQueue.slotWritten(prefetchBlock);

        //get real data
        Block<Chunk> *chunkBlock;
        chunkQueue.getReadSlot(&chunkBlock); //todo we should request specific offset
        printf("Parsing, offset: %ld\n", chunkBlock->data.offset);
        sleep(1); //parsing time
        chunkQueue.slotRead(chunkBlock);
        offset += 1024;
    }

    d.join();

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
