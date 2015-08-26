#ifndef DDC_HDFSUTILS_IURLDOWNLOADER_H
#define DDC_HDFSUTILS_IURLDOWNLOADER_H

#include <stdio.h>
#include <stdint.h>
#include <curl/curl.h>

#include <string>
#include <stdexcept>

#include "blockreader/block.h"

namespace ddc {
namespace hdfsutils {

typedef boost::shared_ptr<std::vector<uint8_t> > BufferPtr;

struct Progress {
    Progress() :
        lastruntime(0.0),
        lastdlbytes(0),
        interval(0),
        lastpercentage(0.0),
        curl(NULL) {

    }
    double lastruntime;
    curl_off_t lastdlbytes;
    int interval;
    float lastpercentage;
    CURL *curl;
};

//struct Buffer {
//    uint8_t *buf;
//    uint64_t size;
//    uint64_t used;
//};

struct Speed {
    uint64_t bytes;
    uint64_t secs;
};

class CurlException: public std::runtime_error {
public:
    explicit CurlException(const std::string &what): std::runtime_error(what) {

    }


private:
};

class CurlLowSpeedException: public CurlException {
public:
    explicit CurlLowSpeedException(const std::string &what): CurlException(what) {

    }


private:
};

class CurlAbortedByCallbackException: public CurlException {
public:
    explicit CurlAbortedByCallbackException(const std::string &what): CurlException(what) {

    }


private:
};

class IUrlDownloader {
public:
    virtual ~IUrlDownloader() {

    }

    virtual int download(const std::string &url, BufferPtr buffer) = 0;
    virtual int download(const std::string &url, const Speed &speed, BufferPtr buffer) = 0;

};

} // namespace hdfsutils
} // namespace ddc

#endif //DDC_HDFSUTILS_IURLDOWNLOADER_H
