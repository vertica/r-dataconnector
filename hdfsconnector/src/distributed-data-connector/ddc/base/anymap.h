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
