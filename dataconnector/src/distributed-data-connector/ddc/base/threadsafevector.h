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


#ifndef DDC_DISTRIBUTOR_THREADSAFEVECTOR_H
#define DDC_DISTRIBUTOR_THREADSAFEVECTOR_H

#include <vector>
#include <boost/thread.hpp>

namespace base {

template<typename T>
class ThreadSafeVector {
public:
    void add(T value) {
        boost::mutex::scoped_lock(m_);
        v_.push_back(value);
    }
    T& get(const int index) {
        boost::mutex::scoped_lock(m_);
        T& value = v_[index];
        return value;
    }

    void erase(int index) {
        boost::mutex::scoped_lock(m_);
        v_.erase(v_.begin() + index);
    }


    size_t size() {
        boost::mutex::scoped_lock(m_);
        return v_.size();
    }

private:
    boost::mutex m_;
    std::vector<T> v_;


};

}
#endif // DDC_DISTRIBUTOR_THREADSAFEVECTOR_H

