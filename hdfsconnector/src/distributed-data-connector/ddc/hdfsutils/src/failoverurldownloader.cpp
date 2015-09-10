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
        }
        catch(CurlException& e) {
            retries--;
            numFailures_++;
        }
        catch(HttpException& e) {
            throw;
        }

    }
    throw std::runtime_error("Unable to dl after retries");

}

} // namespace hdfsutils
} // namespace ddc


