
#ifndef DDC_SCHEDULER_SCHEDULER_H_
#define DDC_SCHEDULER_SCHEDULER_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "base/configurationmap.h"

namespace ddc {
namespace scheduler {

struct Chunk {
    Chunk()
        : id(0),
          protocol(""),
          filename(""),
          start(0),
          end(0) {
    }

    Chunk(const int64_t _id,
          const std::string& _protocol,
          const std::string& _filename,
          const uint64_t _start,
          const uint64_t _end)
        : id(_id),
          protocol(_protocol),
          filename(_filename),
          start(_start),
          end(_end) {

    }

    bool operator==(const Chunk& other) const {
        return ((other.id == id) &&
                (other.protocol == protocol) &&
                (other.filename == filename) &&
                (other.start == start) &&
                (other.end == end));
    }


    int64_t id;
    std::string protocol;
    std::string filename;
    uint64_t start;
    uint64_t end;
};



class WorkerInfo {
public:
    explicit WorkerInfo(const std::string& hostname,
                        const uint64_t port,
                        const uint64_t numExecutors)
        : hostname_(hostname),
          port_(port),
          numExecutors_(numExecutors) {
    }

    std::string hostname() const {
        return hostname_;
    }
    uint64_t port() const {
        return port_;
    }
    uint64_t numExecutors() const {
        return numExecutors_;
    }

private:
    std::string hostname_;
    uint64_t port_;
    uint64_t numExecutors_;
};


typedef std::map<int32_t, boost::shared_ptr<WorkerInfo> > WorkerMap;
typedef std::map<int32_t, std::vector<Chunk> > WorkerChunksMap;


// chunkId -> worker. E.g.:
// chunk0 -> worker0, chunk1 -> worker1 ...
typedef std::map<int64_t, int32_t> ChunkWorkerMap;

class Scheduler {
public:
    virtual ~Scheduler() {

    }

    virtual void configure(base::ConfigurationMap& conf) = 0;

    /**
     * @brief schedule Given a vector of chunks and a map of workers
     *                 schedules the chunks across the workers in the
     *                 most efficient way.
     * @param chunks
     * @param workers
     * @param workerChunksMap
     * @param chunkWorkerMap
     */
    virtual void schedule(std::vector<Chunk>& chunks,
                          WorkerMap& workers,
                          WorkerChunksMap *workerChunksMap,
                          ChunkWorkerMap *chunkWorkerMap) = 0;
};
typedef boost::shared_ptr<Scheduler> SchedulerPtr;

}  // namespace scheduler
}  // namespace ddc

#endif // DDC_SCHEDULER_SCHEDULER_H_
