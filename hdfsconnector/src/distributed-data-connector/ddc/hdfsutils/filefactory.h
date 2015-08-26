#ifndef DDC_HDFSUTILS_FILEFACTORY_H
#define DDC_HDFSUTILS_FILEFACTORY_H

#include <stdexcept>
#include <string>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include "base/ifile.h"
#include "base/scopedfile.h"
#include "base/utils.h"
#include "hdfsfile.h"
#include "hdfsutils/hdfsutils.h"

namespace ddc {
namespace hdfsutils {

class FileFactory
{
public:
    FileFactory();
    ~FileFactory();

    static base::IFilePtr makeFile(const std::string& protocol,
                                   const std::string& filename,
                                   const std::string& mode);
};

}//namespace hdfsutils
}//namespace ddc

#endif // DDC_HDFSUTILS_FILEFACTORY_H
