#include "filefactory.h"


using boost::make_shared;
using boost::shared_ptr;
using std::runtime_error;
using std::string;

namespace ddc{
namespace hdfsutils {

FileFactory::FileFactory()
{

}

FileFactory::~FileFactory()
{

}



base::IFilePtr FileFactory::makeFile(const std::string& protocol,
                                     const std::string& filename,
                                     const std::string& mode){

    if(protocol == "fake") {
        throw std::runtime_error("unsupported");
    }
    else if(isHdfs(protocol)) {
        return base::IFilePtr(new HdfsFile(filename));
    }
    else if( protocol == "file")  {
        return base::IFilePtr(new base::ScopedFile(filename, mode));
    }
    else { //no protocol, return local reader
        return base::IFilePtr(new base::ScopedFile(filename, mode));
    }
}

}//namespace hdfsutils
}//namespace ddc
