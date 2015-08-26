#include "basicurldownloader.h"
#include <string.h>
#include <boost/format.hpp>
#include <glog/logging.h>

#include "ddc/globals.h"

namespace ddc {
namespace hdfsutils {

BasicUrlDownloader::BasicUrlDownloader()
{
    curl_ = curl_easy_init();
}

BasicUrlDownloader::~BasicUrlDownloader()
{
    curl_easy_cleanup(curl_);
    curl_global_cleanup();
}


int BasicUrlDownloader::download(const std::string &url, BufferPtr buffer){
    return downloadSpeedLimit(url, -1,-1, buffer);
}
int BasicUrlDownloader::download(const std::string &url, const Speed &speed, BufferPtr buffer){
    return downloadSpeedLimit(url, (long)speed.bytes, (long)speed.secs, buffer);
}


/* this is how the CURLOPT_XFERINFOFUNCTION callback works */
static int onProgressUpdate(void *p,
                    curl_off_t dltotal, curl_off_t dlnow,
                    curl_off_t ultotal, curl_off_t ulnow)
{
  if (stopDdc) {
    stopDdc = false;
    LOG(INFO) << "Aborting download ...";
    return 1;
  }
  Progress *myp = (Progress *)p;
  CURL *curl = myp->curl;
  double curtime = 0;

  curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &curtime);

  /* under certain circumstances it may be desirable for certain functionality
     to only run every N seconds, in order to do this the transaction time can
     be used */
  if((curtime - myp->lastruntime) >= myp->interval) {
    long time = myp->lastruntime;
    long bytes = myp->lastdlbytes;
    myp->lastruntime = curtime;
    myp->lastdlbytes = dlnow;

    //fprintf(stderr, "TOTAL TIME: %f \r\n", curtime);
    std::string speedUnits = " B/s";
    std::string sizeUnits = " B";
    float speed = (float)(dlnow - bytes)/(float)(curtime - time);
    if(speed >= 1 * 1024 * 1024) {
        speedUnits = "MB/s";
        speed /= 1.0 * 1024.0 * 1024.0;
    }
    else if(speed >= 1 * 1024) {
        speedUnits = "KB/s";
        speed /= 1.0 * 1024.0;
    }

    curl_off_t dltotal2 = dltotal;
    curl_off_t dlnow2  = dlnow;
    if(dltotal >= 1 * 1024 * 1024) {
        sizeUnits = "MB";
        dltotal2 /= 1.0 * 1024.0 * 1024.0;
        dlnow2 /= 1.0 * 1024.0 * 1024.0;
    }
    else if(dltotal >= 1 * 1024) {
        sizeUnits = "KB";
        dltotal2 /= 1.0 * 1024.0;
        dlnow2 /= 1.0 * 1024.0;
    }

    //fprintf(stderr, "Speed: %08.2f %s - %10.2f - %10ld/%10ld %s\r", speed, speedUnits.c_str() , 100 * (float)dlnow2/(float)dltotal2, dlnow2, dltotal2, sizeUnits.c_str());
    if((dlnow2 != 0) && (dltotal2 != 0)) {
        float percentage = 100 * (float)dlnow2/(float)dltotal2;
        if((percentage - myp->lastpercentage) >= 1.0) {
            LOG(INFO) <<  boost::format("Completed %3.2f%%\r")  % (percentage);
            myp->lastpercentage = percentage;
        }
    }
  }

  //fprintf(stderr, "UP: %" CURL_FORMAT_CURL_OFF_T " of %" CURL_FORMAT_CURL_OFF_T
  //        "  DOWN: %" CURL_FORMAT_CURL_OFF_T " of %" CURL_FORMAT_CURL_OFF_T
  //        "\r\n",
  //        ulnow, ultotal, dlnow, dltotal);

  //if(dlnow > STOP_DOWNLOAD_AFTER_THIS_MANY_BYTES)
  //  return 1;
  return 0;
}

/* for libcurl older than 7.32.0 (CURLOPT_PROGRESSFUNCTION) */
static int __onProgressUpdate(void *p,
                   double dltotal, double dlnow,
                   double ultotal, double ulnow){
    return onProgressUpdate(p,
                            (curl_off_t)dltotal,
                            (curl_off_t)dlnow,
                            (curl_off_t)ultotal,
                            (curl_off_t)ulnow);
}

static size_t writeCallback(void *ptr, size_t size, size_t nmemb, void *userdata) {

//    Buffer *b = (Buffer *)userdata;
//    if((b->used + (size * nmemb)) > b->size) return -1;
//    memcpy(b->buf, ptr, size * nmemb);
//    b->used += (size * nmemb);
//    //printf("used: %ld\n", b->used);
//    return size * nmemb;


    BufferPtr b = *((BufferPtr *)userdata);
    size_t oldSize = b->size();

    b->resize(oldSize + (size * nmemb));
    //DLOG(INFO) << "received " << size*nmemb << " bytes, old size: " << oldSize << " resizing to " << oldSize + (size * nmemb) << " newsize: " << b->size();
    //DLOG(INFO) << "copying to offset " << oldSize;
    memcpy(b->data() + oldSize, ptr, size * nmemb);
    return size * nmemb;
}



int BasicUrlDownloader::downloadSpeedLimit(const std::string &url, long lowSpeedLimit, long lowSpeedTime,BufferPtr buffer){

  CURLcode res = CURLE_OK;
  Progress prog;


  if(curl_) {
    prog.lastruntime = 0;
    prog.curl = curl_;

    //curl_easy_setopt(curl, CURLOPT_URL, "http://example.com/");
    DLOG(INFO) << "downloading: " << url;
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());

    curl_easy_setopt(curl_, CURLOPT_BUFFERSIZE, 1*1024*1024);  // TODO magic

    curl_easy_setopt(curl_, CURLOPT_PROGRESSFUNCTION, __onProgressUpdate);
    /* pass the struct pointer into the progress function */
    curl_easy_setopt(curl_, CURLOPT_PROGRESSDATA, &prog);

#if LIBCURL_VERSION_NUM >= 0x072000
    /* xferinfo was introduced in 7.32.0, no earlier libcurl versions will
       compile as they won't have the symbols around.

       If built with a newer libcurl, but running with an older libcurl:
       curl_easy_setopt() will fail in run-time trying to set the new
       callback, making the older callback get used.

       New libcurls will prefer the new callback and instead use that one even
       if both callbacks are set. */

    curl_easy_setopt(curl_, CURLOPT_XFERINFOFUNCTION, onProgressUpdate);
    /* pass the struct pointer into the xferinfo function, note that this is
       an alias to CURLOPT_PROGRESSDATA */
    curl_easy_setopt(curl_, CURLOPT_XFERINFODATA, &prog);
#endif

    curl_easy_setopt(curl_, CURLOPT_NOPROGRESS, 0L);

    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, (void *)&buffer);

    if(lowSpeedLimit != -1)
        curl_easy_setopt(curl_, CURLOPT_LOW_SPEED_LIMIT, lowSpeedLimit);
    if(lowSpeedTime != -1)
        curl_easy_setopt(curl_, CURLOPT_LOW_SPEED_TIME, lowSpeedTime);


    res = curl_easy_perform(curl_);

    if(res != CURLE_OK) {
        if (res == CURLE_OPERATION_TIMEDOUT) {
            throw CurlLowSpeedException(curl_easy_strerror(res));
        }
        else if (res == CURLE_ABORTED_BY_CALLBACK) {
            throw CurlAbortedByCallbackException(curl_easy_strerror(res));
        }
        else {
            throw CurlException(curl_easy_strerror(res));
        }
    }

    /* always cleanup */

  }
  return 0;
}


} // namespace hdfsutils
} // namespace ddc
