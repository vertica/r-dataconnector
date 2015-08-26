#ifndef BASE_IITERATOR_H_
#define BASE_IITERATOR_H_

namespace base{
template<class T>
class IIterator
{
public:
    virtual ~IIterator() {

    }

    virtual T next() = 0;
    virtual bool hasNext() = 0;
};

}  // namespace base

#endif //BASE_IITERATOR_H_

