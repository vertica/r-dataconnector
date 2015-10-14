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


#ifndef BASE_RUNNABLE_H
#define BASE_RUNNABLE_H

#include <boost/any.hpp>
#include <boost/thread.hpp>

namespace base {

class Runnable {
public:
    virtual ~Runnable() {

    }

    void start() {
        thread_ = boost::thread(&Runnable::run, this);
    }


    void cancel() {
        thread_.interrupt();
    }

    void join() {
        thread_.join();
    }

    virtual void run() = 0;


protected:
    boost::thread thread_;
};

} // namespace base

#endif // BASE_RUNNABLE_H
