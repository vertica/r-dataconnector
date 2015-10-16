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

    boost::shared_ptr<base::Cache> fileStatCache_;

    bool configured_;
};



}  // namespace hdfsutils
}  // namespace ddc

#endif // DDC_RECORDPARSER_ORC_HDFSINPUTSTREAM_H_
