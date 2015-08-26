#include "zmqutils.h"
#include <pthread.h>
#include <stdio.h>
#include <boost/format.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

namespace ddc {
namespace distributor {

std::string
s_recv (zmq::socket_t & socket) {

    zmq::message_t message;
    socket.recv(&message);

    return std::string(static_cast<char*>(message.data()), message.size());
}

std::string
s_recv_non_blocking (zmq::socket_t & socket) {
    zmq::message_t message;
    bool rc;
    do {
        if ((rc = socket.recv(&message, ZMQ_DONTWAIT))) {
            return std::string(static_cast<char*>(message.data()), message.size());
        }        
        //boost::this_thread::interruption_point();
        boost::this_thread::sleep(boost::posix_time::millisec(100));
    } while(!rc);

    throw std::runtime_error("empty string");
}


//  Convert string to 0MQ string and send to socket
bool
s_send (zmq::socket_t & socket, const std::string & string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message);
    return (rc);
}

//  Sends string as 0MQ string, as multipart non-terminal
bool
s_sendmore (zmq::socket_t & socket, const std::string & string) {

    zmq::message_t message(string.size());
    memcpy (message.data(), string.data(), string.size());

    bool rc = socket.send (message, ZMQ_SNDMORE);
    return (rc);
}

//  Return current system clock as milliseconds
int64_t
s_clock (void)
{
#if (defined (WIN32))
        FILETIME fileTime;
        GetSystemTimeAsFileTime(&fileTime);
        unsigned __int64 largeInt = fileTime.dwHighDateTime;
        largeInt <<= 32;
        largeInt |= fileTime.dwLowDateTime;
        largeInt /= 10000; // FILETIME is in units of 100 nanoseconds
        return (int64_t)largeInt;
#else
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (int64_t) (tv.tv_sec * 1000 + tv.tv_usec / 1000);
#endif
}

//  Sleep for a number of milliseconds
void
s_sleep (int msecs)
{
    LOG(INFO) << "sleeping for " << msecs << "ms";
#if (defined (WIN32))
    Sleep (msecs);
#else
    struct timespec t;
    t.tv_sec = msecs / 1000;
    t.tv_nsec = (msecs % 1000) * 1000000;
    nanosleep (&t, NULL);
#endif
}

std::string
s_set_id (zmq::socket_t & socket)
{
//    std::stringstream ss;
//    ss << std::hex << std::uppercase
//       << std::setw(4) << std::setfill('0') << within (0x10000) << "-"
//       << std::setw(4) << std::setfill('0') << within (0x10000);
//    socket.setsockopt(ZMQ_IDENTITY, ss.str().c_str(), ss.str().length());
//    return ss.str();

    std::string id = (boost::format("%s-%s") % "ip" % pthread_self()).str();
    socket.setsockopt(ZMQ_IDENTITY, id.c_str(), id.length());
    return id;
}

std::string
s_set_id (zmq::socket_t & socket, const std::string& id)
{

    socket.setsockopt(ZMQ_IDENTITY, id.c_str(), id.length());
    return id;
}


} // namespace distributor
} // namespace ddc
