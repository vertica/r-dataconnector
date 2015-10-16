/*
(c) Copyright 2015 Hewlett Packard Enterprise Development LP

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/



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
