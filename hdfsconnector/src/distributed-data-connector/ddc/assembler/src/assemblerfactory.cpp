#include "assemblerfactory.h"
#include <stdexcept>
#include <boost/make_shared.hpp>
#include "base/utils.h"
#include "devnullassembler.h"
#include "fakeassembler.h"
#include "rdataframeassembler.h"


namespace ddc{
namespace assembler{
AssemblerFactory::AssemblerFactory()
{

}

AssemblerFactory::~AssemblerFactory()
{

}



IAssemblerPtr AssemblerFactory::makeAssembler(const std::string& objectType){
    if(objectType == "fake") return FakeAssemblerPtr(new FakeAssembler());
    else if(objectType == "rdataframe") return RDataFrameAssemblerPtr(new RDataFrameAssembler());
    else if(objectType == "devnull") return DevNullAssemblerPtr(new DevNullAssembler());
    throw std::runtime_error(std::string("No assembler class found for objectType ") + objectType);
}

}//namespace assembler
}//namespace ddc
