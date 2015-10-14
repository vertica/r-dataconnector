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


#ifndef CONFIGURATIONMAP_H
#define CONFIGURATIONMAP_H

#include <map>
#include <string>
#include <boost/any.hpp>

#define GET_PARAMETER(var, type, name) \
    try { \
        var = boost::any_cast<type>(conf[name]); \
    } \
    catch (boost::bad_any_cast& e) { \
        std::ostringstream os; \
        os << "Configuration error. Parameter " << name << \
              " is missing or invalid."; \
        throw std::runtime_error(os.str()); \
    }


namespace base {

typedef std::map<std::string, boost::any> ConfigurationMap;

} // namespace base

#endif // CONFIGURATIONMAP_H
