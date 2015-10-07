#ifndef BASE_UTILS_H
#define BASE_UTILS_H

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <glob.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <vector>

#include <boost/any.hpp>

namespace base {
namespace utils {

    std::string getExtension(const std::string &file);
    std::string getProtocol(const std::string &file);

    bool areEqual(const boost::any& lhs, const boost::any& rhs);
    bool areEqual(const std::vector<boost::any>&a, const std::vector<boost::any>&b);

    std::string readFile(const std::string& path);

    template <typename T>
    std::string to_string(T value)
    {
        std::ostringstream os ;
        os << value ;
        return os.str() ;
    }

    std::map<std::string, bool> ipAddresses();
    void ipAddresses(std::vector<std::string>& ipAddresses);

    std::string removeSubstrs(const std::string& s, const std::string& p);

    std::string stripProtocol(const std::string& url);

    std::vector<std::string> globpp(const std::string& pat);

    std::string hostnameToIpAddress(const std::string& hostname);


    void buffer2file(uint8_t *buffer,
                     const uint64_t size,
                     const std::string filename);

}  // namespace utils
}  // namespace base

std::ostream &operator<<(std::ostream &stream, const std::vector<uint64_t> &v);

#endif // BASE_UTILS_H
