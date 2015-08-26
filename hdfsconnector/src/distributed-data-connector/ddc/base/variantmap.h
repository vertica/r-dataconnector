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
