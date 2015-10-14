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

    typedef std::map<int32_t, std::pair<std::string,std::string> > CsvSchema;
    typedef std::map<int32_t, std::pair<std::string,std::string> >::iterator CsvSchemaIt;
    typedef std::map<int32_t, std::pair<std::string,std::string> >::const_iterator CsvSchemaConstIt;

    CsvSchema parseSchema(const std::string& schema);
    std::string schema2string(const CsvSchema& schema);
    std::vector<std::string> schema2colnames(const std::string& schema);

    boost::any ddc_read(const std::string &url,
                    const std::string& objectType,
                    base::ConfigurationMap& conf);

    Rcpp::List create_plan(const std::string& url,
                           base::ConfigurationMap& options,
                           const ddc::scheduler::WorkerMap& workerMap);

} // namespace ddc

#endif // DDC_DDC_H
