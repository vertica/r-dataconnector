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


#ifndef DDC_HDFSUTILS_FAILOVERURLDOWNLOADER_H
#define DDC_HDFSUTILS_FAILOVERURLDOWNLOADER_H

#include <unistd.h>

#include <vector>
#include <string>

#include <glog/logging.h>

#include "hdfsutils/basicurldownloader.h"


namespace ddc {
namespace hdfsutils {


class FailoverUrlDownloader //: public IUrlDownloader
{
public:
    FailoverUrlDownloader();

    ~FailoverUrlDownloader();

    int download(const std::vector<std::string> &urls, BufferPtr buffer);
    int download(const std::vector<std::string> &urls, const Speed &speed, BufferPtr buffer);

    std::vector<std::string> failedUrls();
    void clearFailedUrls();
    uint32_t numFailures() const {
        return numFailures_;
    }
    static const int NUM_RETRIES = 3; // TODO make this configurable

private:
    int downloadWithRetries(const std::string &url, const Speed &speed, BufferPtr buffer);

    BasicUrlDownloader basicUrlDownloader_;

    std::vector<std::string> failedUrls_;
    uint32_t retries_;
    uint32_t numFailures_;

};

} // namespace hdfsutils
} // namespace ddc

#endif // DDC_HDFSUTILS_FAILOVERURLDOWNLOADER_H
