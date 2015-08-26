#ifndef DDC_HDFSUTILS_BASICURLDOWNLOADER_H
#define DDC_HDFSUTILS_BASICURLDOWNLOADER_H

#include "hdfsutils/iurldownloader.h"

namespace ddc{
namespace hdfsutils {


class BasicUrlDownloader : public IUrlDownloader{

public:
    BasicUrlDownloader();
    ~BasicUrlDownloader();

    int download(const std::string &url, BufferPtr buffer);
    int download(const std::string &url, const Speed &speed, BufferPtr buffer);
private:
    int downloadSpeedLimit(const std::string &url, long lowSpeedLimit, long lowSpeedTime, BufferPtr buffer);

    CURL *curl_;

}; //BasicUrlDownloader

} // namespace hdfsutils
} // namespace ddc

#endif // DDC_HDFSUTILS_BASICURLDOWNLOADER_H
