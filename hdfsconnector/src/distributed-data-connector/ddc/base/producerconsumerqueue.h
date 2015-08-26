#ifndef BASE_PRODUCERCONSUMERQUEUE_H_
#define BASE_PRODUCERCONSUMERQUEUE_H_

#include <stdint.h>
#include <stdio.h>
#include <boost/thread.hpp>

namespace base {

/**
 * A block is a class to exchange information between pipeline stages.
 * T is the type of data to be enchanged. Can be an integer or a complex object.
 */
template <typename T>
struct Block
{
    uint64_t id;
    T data;

    bool shutdownStage;
    int64_t index;
    bool locked;
    bool hasValidData;

    explicit Block()
        : id(0),
          shutdownStage(false),
          index(0),
          locked(false),
          hasValidData(false)
    {
    }

    explicit Block(uint64_t id0) :
        id(id0),
        shutdownStage(false),
        index(0),
        locked(false),
        hasValidData(false)
    {
    }

    ~Block()
    {
    }

    void print()
    {
        printf("\tid:        %d\n", id);
    }
};



/**
 * base::ProducerConsumerQueue holds blocks between pipeline stages. See block class above.
 * Producers write blocks to the base::ProducerConsumerQueue using the getWriteSlot() and slotWritten() API.
 * Consumers read blocks from the base::ProducerConsumerQueue using the getReadSlot() and slotRead() API.
 */
template <typename T>
class ProducerConsumerQueue
{
public:

    ProducerConsumerQueue() : configured_(false) {
    }

    ~ProducerConsumerQueue()
    {
        clean();
    }

    void clean()
    {
        if (configured_)
        {
            for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
            {
                delete *it;
            }
            configured_ = false;
            blocks_.clear();
        }
    }


    void configure(uint32_t numBuffers) {
        clean();  // clean if previously configured
        size_ = numBuffers;

        blocks_.reserve(numBuffers);
        for (uint32_t i = 0; i < numBuffers; i++)
        {
            T *block = new T(i);
            blocks_.push_back(block);
        }

        lastBufferGiven_ = -1;
        lastBufferAdded_ = -1;
        configured_ = true;

    }


    void getNextFreePosition(T **block, bool &hasFreePositions)
    {
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if (!((*it)->locked))
            {
                (*it)->locked = true;
                if ((*it)->hasValidData)
                {
                    assert(0);
                    printf("%p: Error!!! Locking buffer and hasValidData is 1\n", this);
                }
                *block = *it;
                hasFreePositions = true;
                return;
            }
            else
            {
            }
        }
        hasFreePositions = false;
        return;
    }
    bool getNextFreePosition(T **block)
    {
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if (!((*it)->locked))
            {
                (*it)->locked = true;
                if ((*it)->hasValidData)
                {
                    assert(0);
                    printf("%p: Error!!! Locking buffer and hasValidData is 1\n", this);
                }
                *block = *it;
                return true;
            }
            else
            {
            }
        }
        return false;
    }

    void getNextValidPosition(T **block, bool &hasValidData)
    {
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if ((*it)->hasValidData && ((*it)->index == (lastBufferGiven_ + 1)))
            {

                lastBufferGiven_++;
                if (!(*it)->locked)
                {
                    assert(0);
                    printf("%p: Error getting valid data and buffer is not locked!!! \n", this);
                }
                (*it)->hasValidData = false;
                *block = *it;
                hasValidData = true;
                return;
            }
        }
        hasValidData = false;
        return;
    }

    void getNextValidPosition(T **block, uint64_t id, bool &hasValidData)
    {
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if ((*it)->hasValidData)
            {
                if((*it)->id != id) {
                    printf("id mismatch: looking for: %ld, found: %ld\n", id, (*it)->id);
                    continue;
                }
                lastBufferGiven_++;
                if (!(*it)->locked)
                {
                    assert(0);
                    printf("%p: Error getting valid data and buffer is not locked!!! \n", this);
                }
                (*it)->hasValidData = false;
                *block = *it;
                hasValidData = true;
                return;
            }
        }
        hasValidData = false;
        printf("no valid found\n");
        return;
    }

    bool getNextValidPosition(T **block)
    {
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if ((*it)->hasValidData && ((*it)->index == (lastBufferGiven_ + 1)))
            {

                lastBufferGiven_++;
                if (!(*it)->locked)
                {
                    assert(0);
                    printf("%p: Error getting valid data and buffer is not locked!!! \n", this);
                }
                (*it)->hasValidData = false;
                *block = *it;
                return true;
            }
        }
        return false;
    }

    //
    // reserve a block
    //
    void getWriteSlot(T **block)
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        //mutex_.lock();

        bool hasFreePositions = false;
        getNextFreePosition(block, hasFreePositions);
        while (!hasFreePositions)
        {
            //printf("%p: Waiting for free slots...\n", this);
            bufferHasFreeSlots_.wait(lock);
            getNextFreePosition(block, hasFreePositions);
        }
    }

    //
    // reserve a block
    // returns 1 on success 0 on failure
    //
    bool tryGetWriteSlot(T **block)
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        //mutex_.lock();

        bool hasFreePositions = false;
        getNextFreePosition(block, hasFreePositions);
        while (!hasFreePositions)
        {
            //printf("%p: Waiting for free slots...\n", this);
            boost::system_time const timeout = boost::get_system_time() + boost::posix_time::milliseconds(1);
            if (bufferHasFreeSlots_.timed_wait(lock, timeout, boost::bind(&base::ProducerConsumerQueue<T>::getNextFreePosition, this, block))) {
                return true;
            }
            else {
                //std::cout << "timeout while write" << std::endl;
                return false;
            }
            
        }
        return true;//success
    }


    //
    // mark the block has valid data
    //

    void slotWritten(T *block)
    {
        mutex_.lock();

        bool found = false;
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if ((*it)->id == block->id)
            {
                found = true;
                (*it)->hasValidData = true;
                lastBufferAdded_++;
                (*it)->index = lastBufferAdded_;

                if (!(*it)->locked)
                {
                    assert(0);
                    printf("%p: Error!!! setting hasValidData and locked is false\n", this);
                }

                bufferHasValidData_.notify_all();
            }
        }
        if (!found)
        {
            printf("%p: slotWritten -> buffer %ld not found\n", this, block->id);
        }

        mutex_.unlock();
    }

    //
    // get a buffer with valid data
    //
    void getReadSlot(T **block)
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        bool hasValidData = false;
        getNextValidPosition(block, hasValidData);
        while (!hasValidData)
        {
            //printf("%p: Waiting for slots with valid data...\n", this);
            bufferHasValidData_.wait(lock);
            getNextValidPosition(block, hasValidData);
        }
    }

    //
    // get a specific buffer
    //
    void getReadSlot(T **block, uint64_t id)
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        bool hasValidData = false;
        getNextValidPosition(block, id, hasValidData);
        while (!hasValidData)
        {
            //printf("%p: Waiting for slots with valid data...\n", this);
            bufferHasValidData_.wait(lock);
            getNextValidPosition(block, id, hasValidData);
        }
    }

    
    bool tryGetReadSlot(T **block)
    {
        boost::unique_lock<boost::mutex> lock(mutex_);
        bool hasValidData = false;
        getNextValidPosition(block, hasValidData);
        while (!hasValidData)
        {
            boost::system_time const timeout = boost::get_system_time() + boost::posix_time::milliseconds(1);
            if (bufferHasValidData_.timed_wait(lock, timeout, boost::bind(&base::ProducerConsumerQueue<T>::getNextValidPosition, this, block))){
                return true;
            }
            else {
                return false;
            }
        }
        return true;
    }


    //
    // mark the buffer as read so base::ProducerConsumerQueue can reuse it
    //
    void slotRead(T *block)
    {
        mutex_.lock();
        bool found = false;
        for (typename std::vector<T *>::iterator it = blocks_.begin(); it != blocks_.end(); it++)
        {
            if ((*it)->id == block->id)
            {
                found = true;
                if (!(*it)->locked)
                {
                    assert(0);
                    printf("%p: Error!!! locked was 0 in slotRead\n", this);
                }
                (*it)->locked = false;
                if ((*it)->hasValidData)
                {
                    assert(0);
                    printf("%p: Error!!! Freeing buffer and hasValidData is 1\n", this);
                }
                bufferHasFreeSlots_.notify_all();
            }
        }
        if (!found)
        {
            printf("%p: slotRead -> buffer %ld not found\n", this, block->id);
        }
        mutex_.unlock();
    }
private:

    bool configured_;
    std::vector<T *> blocks_;
    boost::condition_variable bufferHasValidData_;
    boost::condition_variable bufferHasFreeSlots_;
    boost::mutex mutex_;
    uint64_t size_;
    int64_t lastBufferGiven_;
    int64_t lastBufferAdded_;
};

}  // namespace base

#endif  // BASE_PRODUCERCONSUMERQUEUE_H_
