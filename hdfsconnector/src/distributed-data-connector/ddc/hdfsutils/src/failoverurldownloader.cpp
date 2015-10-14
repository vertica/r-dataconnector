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


#include "failoverurldownloader.h"

namespace ddc {
namespace hdfsutils {

FailoverUrlDownloader::FailoverUrlDownloader()
    : retries_(NUM_RETRIES),
      numFailures_(0) {

}

FailoverUrlDownloader::~FailoverUrlDownloader(){

}

int FailoverUrlDownloader::download(const std::vector<std::string> &urls, BufferPtr buffer){
    Speed s;
    s.bytes = -1;
    s.secs = -1;
    return download(urls, s, buffer);
}
int FailoverUrlDownloader::download(const std::vector<std::string> &urls, const Speed &speed, BufferPtr buffer){
    for(uint64_t i = 0; i < urls.size(); i++) {
        try {
            return downloadWithRetries(urls[i],speed, buffer);
        }
        catch (CurlAbortedByCallbackException& e) {
            throw std::runtime_error("User cancelled operation. Exiting ...");
        }
        catch (HttpException& e) {
            throw;
        }
        catch(std::runtime_error &e) {
            //logDebug ...
            failedUrls_.push_back(urls[i]);
        }
    }
    throw std::runtime_error("Unable to dl from any mirror");

}
std::vector<std::string> FailoverUrlDownloader::failedUrls() {
    return failedUrls_;
}

void FailoverUrlDownloader::clearFailedUrls() {
    failedUrls_.clear();
}

int FailoverUrlDownloader::downloadWithRetries(const std::string &url, const Speed &speed, BufferPtr buffer){

    int retries = retries_;
    while(retries > 0) {
        try {
            int res = basicUrlDownloader_.download(url, speed, buffer);
            return res;
        }
        catch (CurlAbortedByCallbackException& e) {
            throw;
        }

        catch(CurlLowSpeedException& e) {
            retries--;
            numFailures_++;
            LOG(ERROR) << "Transfer failed, retrying in 5 secs...";
            sleep(5);
        }
        catch(CurlException& e) {
            retries--;
            numFailures_++;
            LOG(ERROR) << "Transfer failed, retrying in 5 secs...";
            sleep(5);

        }
        catch(HttpException& e) {
            throw;
        }

    }
    throw std::runtime_error("Unable to dl after retries");

}

} // namespace hdfsutils
} // namespace ddc


