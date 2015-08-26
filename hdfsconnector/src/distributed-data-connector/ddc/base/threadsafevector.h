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

