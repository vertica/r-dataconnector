
#include "schedulerfactory.h"
#include "hdfsutils/hdfsutils.h"

namespace ddc {
namespace scheduler {

SchedulerPtr SchedulerFactory::createScheduler(const std::string &protocol, const std::string &extension) {
    if (hdfsutils::isHdfs(protocol)) {
        return boost::shared_ptr<Scheduler>(new HdfsLocalityScheduler);
    }
    else {
        return boost::shared_ptr<Scheduler>(new RoundRobinScheduler);
    }
}

}  // namespace scheduler
}  // namespace ddc
