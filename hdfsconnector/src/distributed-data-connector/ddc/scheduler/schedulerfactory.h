
#ifndef DDC_SCHEDULER_SCHEDULERFACTORY_H_
#define DDC_SCHEDULER_SCHEDULERFACTORY_H_

#include "scheduler/scheduler.h"
#include "scheduler/hdfslocalityscheduler.h"
#include "scheduler/roundrobinscheduler.h"

namespace ddc {
namespace scheduler {

class SchedulerFactory {
 public:
    static SchedulerPtr createScheduler(const std::string& protocol,
                                        const std::string& extension);

 private:
};

}  // namespace scheduler
}  // namespace ddc

#endif // DDC_SCHEDULER_SCHEDULERFACTORY_H_
