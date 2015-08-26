#ifndef DDC_BASE_IOBSERVER_H_
#define DDC_BASE_IOBSERVER_H_

namespace base{
template<class T>
class IObserver
{
public:
    virtual ~IObserver() {

    }

    virtual void update(T) = 0;
};
}//namespace base
#endif //DDC_BASE_IOBSERVER_H_
