#ifndef BASE_ANYMAP_H
#define BASE_ANYMAP_H

#include <map>
#include <utility>
#include <boost/any.hpp>


namespace base {

template <class T>
class AnyMap
{
public:
    AnyMap();
    ~AnyMap();
    boost::any get(T key);
    boost::any get(T& key);
    void insert(std::pair<T, boost::any> pair);
    void remove(T& key);
private:
    std::map<T, boost::any> AnyMap_;

};

template <class T>
AnyMap<T>::AnyMap()
{

}

template <class T>
AnyMap<T>::~AnyMap()
{

}

template<class T>
boost::any AnyMap<T>::get(T key)
{
    return AnyMap_[key];

}

template<class T>
boost::any AnyMap<T>::get(T& key)
{
    return AnyMap_[key];

}

template <class T>
void AnyMap<T>::insert(std::pair<T, boost::any> pair)
{
    AnyMap_.insert(pair);

}
template <class T>
void AnyMap<T>::remove(T &key)
{
    AnyMap_.erase(key);

}

}//namespace base
#endif // BASE_ANYMAP_H
