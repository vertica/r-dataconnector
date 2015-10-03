#include "utils.h"

namespace base{
namespace utils{


std::string getExtension(const std::string &file)
{
      return file.substr(file.find_last_of(".") + 1);

}

std::string getProtocol(const std::string &file)
{
      std::string protocol = file.substr(0, file.find_first_of(":"));
      if(protocol == file) protocol = "";
      return protocol;

}

bool areEqual(const boost::any& lhs, const boost::any& rhs)
{
    if (lhs.type() != rhs.type()) {
        std::cout << "different types" << std::endl;
        return false;
    }


    if (lhs.type() == typeid(std::string)) {

        bool res = boost::any_cast<std::string>(lhs) == boost::any_cast<std::string>(rhs);
        if(!res) {
            std::cout << "string mismatch-> " << boost::any_cast<std::string>(lhs) <<
                         " vs " << boost::any_cast<std::string>(rhs)  << std::endl;
        }
        else {
            //std::cout << "strings equal" << std::endl;
        }
        return res;
    }

    if (lhs.type() == typeid(int32_t)) {
        bool res = boost::any_cast<int32_t>(lhs) == boost::any_cast<int32_t>(rhs);
        if(!res) {
            std::cout << "int32_t mismatch-> " << boost::any_cast<int32_t>(lhs) <<
                         " vs " << boost::any_cast<int32_t>(rhs)  << std::endl;
        }
        else {
            //std::cout << "int32_t equal" << std::endl;
        }
        return res;
    }

    if (lhs.type() == typeid(int64_t)) {
        bool res = boost::any_cast<int64_t>(lhs) == boost::any_cast<int64_t>(rhs);
        if(!res) {
            std::cout << "int64_t mismatch-> " << boost::any_cast<int64_t>(lhs) <<
                         " vs " << boost::any_cast<int64_t>(rhs)  << std::endl;
        }
        else {
            //std::cout << "int64_t equal" << std::endl;
        }
        return res;
    }

    if (lhs.type() == typeid(double)) {
        bool res = boost::any_cast<double>(lhs) == boost::any_cast<double>(rhs);
        if(!res) {
            std::cout << "double mismatch-> " << boost::any_cast<double>(lhs) <<
                         " vs " << boost::any_cast<double>(rhs)  << std::endl;
        }
        else {
            //std::cout << "double equal" << std::endl;
        }
        return res;
    }

    // ...

    throw std::runtime_error("comparison of any unimplemented for type");
}

bool areEqual(const std::vector<boost::any>&a, const std::vector<boost::any>&b) {
    if(a.size() != b.size()) {
        std::cout << "vectors are of different size" << std::endl;
        return false;
    }
    for(uint64_t i = 0; i < a.size(); i++) {
        //std::cout << i << std::endl;
        if(!areEqual(a[i],b[i])) return false;
    }
    return true;
}

std::string readFile(const std::string& path) {
    std::ifstream t(path.c_str());
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());
    return str;
}



std::map<std::string, bool> ipAddresses() {
    struct ifaddrs * ifAddrStruct=NULL;
    struct ifaddrs * ifa=NULL;
    void * tmpAddrPtr=NULL;
    std::map<std::string, bool> ipAddresses;

    getifaddrs(&ifAddrStruct);

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }
        if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
            // is a valid IP4 Address
            tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            //printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            ipAddresses[std::string(addressBuffer)] = true;
        } else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
            // is a valid IP6 Address
            tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            //printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            ipAddresses[std::string(addressBuffer)] = true;
        }
    }
    if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);

    return ipAddresses;
}

std::string removeSubstrs(const std::string& s, const std::string& p) {
    std::string res(s);
    std::string::size_type n = p.length();

   for (std::string::size_type i = res.find(p); i != std::string::npos; i = res.find(p)) {
      res.erase(i, n);
   }
   return res;
}

void ipAddresses(std::vector<std::string> &ipAddresses)
{
    struct ifaddrs * ifAddrStruct=NULL;
    struct ifaddrs * ifa=NULL;
    void * tmpAddrPtr=NULL;

    getifaddrs(&ifAddrStruct);

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) {
            continue;
        }
        if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
            // is a valid IP4 Address
            tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            //printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            ipAddresses.push_back(std::string(addressBuffer));
        } else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
            // is a valid IP6 Address
            tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            //printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            ipAddresses.push_back(std::string(addressBuffer));
        }
    }
    if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);

}

std::string stripProtocol(const std::string &url) {
    std::string protocol = base::utils::getProtocol(url);

    std::string filename;
    if(protocol != "") {
        filename = base::utils::removeSubstrs(url, protocol + "://");
    }
    else {
        filename = url;
    }
    return filename;
}

std::vector<std::string> globpp(const std::string &pat) {
    using namespace std;
    glob_t glob_result;
    glob(pat.c_str(),GLOB_TILDE,NULL,&glob_result);
    vector<string> ret;
    for(unsigned int i=0;i<glob_result.gl_pathc;++i){
        ret.push_back(string(glob_result.gl_pathv[i]));
    }
    globfree(&glob_result);
    return ret;
}

std::string hostnameToIpAddress(const std::string &hostname)
{
    struct hostent *he;
    struct in_addr **addr_list;

    if ((he = gethostbyname( hostname.c_str())) == NULL)
    {
        std::ostringstream os;
        os << "Unable to resolve hostname " << hostname << " (" << hstrerror(h_errno) << ").";
        throw std::runtime_error(os.str());
    }

    addr_list = (struct in_addr **) he->h_addr_list;

    for(int i = 0; addr_list[i] != NULL; i++)
    {
        //Return the first one;
        return std::string(inet_ntoa(*addr_list[i]));
    }
    std::ostringstream os;
    os << "Unable to resolve hostname " << hostname;
    throw std::runtime_error(os.str());
}


} //namespace utils
} //namespace base


std::ostream &operator<<(std::ostream &stream, const std::vector<uint64_t> &v) {
    for (uint64_t i = 0; i < v.size(); ++i) {
        stream << v[i] << ", ";
    }
}
