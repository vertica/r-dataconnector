#ifndef CONFIGURATIONMAP_H
#define CONFIGURATIONMAP_H

#include <map>
#include <string>
#include <boost/any.hpp>

namespace base {

typedef std::map<std::string, boost::any> ConfigurationMap;

} // namespace base

#endif // CONFIGURATIONMAP_H
