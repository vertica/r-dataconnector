
#ifndef DDC_RECORDPARSER_ORC_HDFSINPUTSTREAM_H_
#define DDC_RECORDPARSER_ORC_HDFSINPUTSTREAM_H_

#include <string>
#include <glog/logging.h>
#include "base/ifile.h"
#include "hdfsutils/failoverurldownloader.h"
#include "hdfsutils/hdfsblocklocator.h"
#include "hdfsutils/hdfsfile.h"
#include "orc/OrcFile.hh"

namespace ddc {
namespace hdfsutils {

class HdfsInputStream : public orc::InputStream {
 public:

    explicit HdfsInputStream(const std::string& url);
    ~HdfsInputStream();

    void configure(base::ConfigurationMap& conf);

    /**
     * Get the total length of the file in bytes.
     */
    uint64_t getLength() const;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer.
     * @param offset the position in the file to read from
     * @param length the number of bytes to read
     * @param buffer a Buffer to reuse from a previous call to read. Ownership
     *    of this buffer passes to the InputStream object.
     * @return the buffer with the requested data. The client owns the Buffer.
     */
    orc::Buffer* read(uint64_t offset,
                 uint64_t length,
                 orc::Buffer* buffer);

    /**
     * Get the name of the stream for error messages.
     */
    const std::string& getName() const;

 private:
    std::string url_;
    std::string hdfsConfigurationFile_;
    base::FileStatus stat_;
    hdfsutils::HdfsFilePtr hdfsFile_;

    bool configured_;
};



}  // namespace hdfsutils
}  // namespace ddc

#endif // DDC_RECORDPARSER_ORC_HDFSINPUTSTREAM_H_
