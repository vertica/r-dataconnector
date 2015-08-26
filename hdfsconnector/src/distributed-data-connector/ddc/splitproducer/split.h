#ifndef DDC_SPLITPRODUCER_SPLIT_H
#define DDC_SPLITPRODUCER_SPLIT_H

#include <stdint.h>
#include "blockreader/block.h"

namespace ddc {
namespace splitproducer {

struct Split :  public blockreader::Block {
    Split() : Block(){

    }
    Split(uint8_t* b, uint64_t u, uint64_t s) : Block(b,u,s){

    }

    explicit Split(const boost::shared_ptr<std::string>& s) : Block(s) {
    }

    explicit Split(const boost::shared_ptr<std::vector<uint8_t> >& v) : Block(v) {
    }

    //TODO what if buffer wasn't created in this object?
    virtual ~Split() {
    }

};

typedef boost::shared_ptr<Split> SplitPtr;

} // namespace splitproducer
} // namespace ddc
#endif // DDC_SPLITPRODUCER_SPLIT_H

