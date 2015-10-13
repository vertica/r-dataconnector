
#include "cache.h"

namespace base {

void Cache::set(const std::string &key, const boost::any &value) {
    map_[key] = value;
}

boost::any Cache::get(const std::string &key) {
    if (map_.find(key) == map_.end()) {
        std::ostringstream os;
        os << "key " << key << " not found in the case";
        throw std::runtime_error(os.str());
    }
    return map_[key];
}

bool Cache::contains(const std::string &key) {
    return map_.find(key) != map_.end();
}

}  // namespace base
