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
