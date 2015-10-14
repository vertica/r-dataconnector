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
