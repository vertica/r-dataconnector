#ifndef DDC_DISTRIBUTOR_ZMQUTILS_H
#define DDC_DISTRIBUTOR_ZMQUTILS_H

#include <zmq.hpp>

#define within(num) (int) ((float) (num) * random () / (RAND_MAX + 1.0))

namespace ddc {
namespace distributor {




std::string
s_recv (zmq::socket_t & socket);

std::string
s_recv_non_blocking (zmq::socket_t & socket);


//  Convert string to 0MQ string and send to socket
bool
s_send (zmq::socket_t & socket, const std::string & string);

//  Sends string as 0MQ string, as multipart non-terminal
bool
s_sendmore (zmq::socket_t & socket, const std::string & string);

int64_t
s_clock (void);

void
s_sleep (int msecs);

std::string
s_set_id (zmq::socket_t & socket);
std::string
s_set_id (zmq::socket_t & socket, const std::string& id);

} // namespace distributor
} // namespace ddc

#endif //  DDC_DISTRIBUTOR_ZMQUTILS_H
