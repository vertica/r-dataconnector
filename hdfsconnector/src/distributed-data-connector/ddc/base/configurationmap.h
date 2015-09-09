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
