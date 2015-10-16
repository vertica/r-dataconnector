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


#ifndef BASE_VARIANTMAP_H
#define BASE_VARIANTMAP_H

#include <map>
#include <utility>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>

namespace base {

struct Date{
    Date(int h, int s) : hour(h),secs(s) {

    }

    int hour;
    int secs;
};

typedef boost::variant<std::string, int32_t, boost::shared_ptr<base::Date> > MyVariant;

template <class T>
class VariantMap
{
public:
    VariantMap();
    ~VariantMap();

    MyVariant get(T key);
    MyVariant get(T& key);
    void insert(std::pair<T, MyVariant> pair);
    void remove(T& key);
private:
    std::map<T, MyVariant> variantMap_;

};

template <class T>
VariantMap<T>::VariantMap()
{

}

template <class T>
VariantMap<T>::~VariantMap()
{

}

template<class T>
MyVariant VariantMap<T>::get(T key)
{
    return variantMap_[key];

}

template<class T>
MyVariant VariantMap<T>::get(T& key)
{
    return variantMap_[key];

}

template <class T>
void VariantMap<T>::insert(std::pair<T, MyVariant> pair)
{
    variantMap_.insert(pair);

}
template <class T>
void VariantMap<T>::remove(T &key)
{
    variantMap_.erase(key);

}

}//namespace base
#endif // BASE_VARIANTMAP_H
