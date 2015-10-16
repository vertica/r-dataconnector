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


#ifndef BASE_CACHE_H_
#define BASE_CACHE_H_

#include <map>
#include <sstream>
#include <stdexcept>

#include <boost/any.hpp>

namespace base {

class Cache {
 public:
    void set(const std::string& key, const boost::any& value);

    boost::any get(const std::string& key);

    bool contains(const std::string& key);

 private:
    std::map<std::string, boost::any> map_;

};

}  // namespace base

#endif // BASE_CACHE_H_
