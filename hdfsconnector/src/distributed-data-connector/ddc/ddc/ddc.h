#ifndef DDC_DDC_H
#define DDC_DDC_H

#include <stdint.h>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <boost/any.hpp>

#include "base/configurationmap.h"
#include "scheduler/chunkscheduler.h"

#include <Rcpp.h>

namespace ddc {


std::map<int32_t, std::pair<std::string, std::string> > parseSchema(const std::string& schema);
boost::any ddc_read(const std::string &url,
                    const std::string& objectType,
                    base::ConfigurationMap& conf);

Rcpp::List create_plan(const std::string& url,
                       base::ConfigurationMap& options,
                       const ddc::scheduler::WorkerMap& workerMap);

} // namespace ddc

#endif // DDC_DDC_H
