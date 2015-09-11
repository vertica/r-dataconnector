#ifndef DDC_ASSEMBLER_IASSEMBLER_H
#define DDC_ASSEMBLER_IASSEMBLER_H

#include <string>
#include <boost/any.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>
#include "base/configurationmap.h"
#include "base/iobserver.h"
#include "recordparser/irecordparser.h"

namespace ddc {
namespace assembler {

/**
 * @brief Assembler records into a final object, e.g. an R dataframe
 */
class IAssembler: public base::IObserver<int32_t> {
public:
    virtual ~IAssembler(){
    }
    virtual void configure(base::ConfigurationMap &conf) = 0;
    virtual boost::any getObject() = 0;
    /**
     * @brief Called by the lower layer (record parser) every time a section
     *        of the file is completed.
     * @param level Used to indicate which part was completed.
     *        In CSV level 0 indicates a row has been consumed.
     *        In ORC level 0 indicates a column has been consumed and
     *        level 1 indicates a stripe has been consumed.
     */
    virtual void update(int32_t level) = 0;
};

typedef boost::shared_ptr<IAssembler> IAssemblerPtr;

typedef boost::shared_ptr<std::vector<bool> > BoolVectorPtr;
typedef boost::shared_ptr<std::vector<int32_t> > Int32VectorPtr;
//typedef boost::shared_ptr<std::vector<int64_t> > Int64VectorPtr;
typedef boost::shared_ptr<std::vector<double> > DoubleVectorPtr;
typedef boost::shared_ptr<std::vector<std::string> > CharacterVectorPtr;


}//namespace assembler
}//namespace ddc

#endif// DDC_BASE_IASSEMBLER_H
