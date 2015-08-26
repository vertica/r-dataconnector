#include "hdfsfile.h"
#include <glog/logging.h>

namespace ddc {
namespace hdfsutils {

HdfsFile::HdfsFile(const std::string& filename)
    : conf_(NULL),
      fs_(NULL),
      f_(NULL),
      overwrite_(false),
      configured_(false) {
    filename_ = filename;
}

void HdfsFile::configure(base::ConfigurationMap &conf) {
    try {
        hdfsConfigurationFile_ = boost::any_cast<std::string>(conf["hdfsConfigurationFile"]);
    }
    catch (...) {
        std::ostringstream os;
        os << "Need to specify hdfsConfigurationFile in options";
        LOG(ERROR) << os.str();
        throw std::runtime_error(os.str());
    }

    try {
        overwrite_ = boost::any_cast<bool>(conf["overwrite"]);
    }
    catch(...) {
        // PASS
    }

    /* Setup webhdfs config */
    DLOG(INFO) << "Reading HDFS config: " << hdfsConfigurationFile_;

    conf_ = webhdfs_conf_load(hdfsConfigurationFile_.c_str());
    if(!conf_) {
        throw std::runtime_error("error reading hdfs config");
    }

    /* Connect to WebHDFS */
    fs_ = webhdfs_connect(conf_);
    if(!fs_) {
        throw std::runtime_error("error in webhdfs_connect");
    }

    f_ = webhdfs_file_open(fs_, filename_.c_str());

    if(!f_) {
        throw std::runtime_error("error in webhdfs_file_open");
    }
    configured_ = true;
}

size_t HdfsFile::read(void *buffer, const size_t bytes) {
    throw std::runtime_error("not implemented");

}

size_t HdfsFile::pread(void *buffer, const size_t bytes, const size_t offset) {
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    return webhdfs_file_pread(f_, buffer, bytes, offset);
}

struct data {
    data() : buffer(NULL), offset(0), nbytes(0) {

    }

    void* buffer;
    size_t offset;
    size_t nbytes;
};

static size_t __upload_func (void *ptr, size_t size, void *data) {
    struct data *p = (struct data *)data;
    size_t nbytes = size;
    if (p->nbytes < nbytes)
        nbytes = p->nbytes;

    DLOG(INFO) << "Uploading " << nbytes <<
                  " bytes, offset: " <<
                  p->offset;
    memcpy(ptr, ((uint8_t *)p->buffer) + p->offset, nbytes);
    p->nbytes -= nbytes;
    p->offset += nbytes;
    return(nbytes);
}


size_t HdfsFile::write(void *buffer, const size_t nbytes) {
    if(!configured_) {
        LOG(ERROR) << "Need to configure first";
        throw std::runtime_error("Need to configure first");
    }
    data d;
    d.buffer = buffer;
    d.nbytes = nbytes;
    int res;
    if (res = webhdfs_file_create(fs_, filename_.c_str(), overwrite_, __upload_func, &d)) {
        throw std::runtime_error("Error writing file");
    }
    return nbytes;  // on success return nbytes
}

int HdfsFile::eof() {
    throw std::runtime_error("not implemented");
}

base::FileStatus HdfsFile::stat()
{
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    webhdfs_fstat_t *stat = webhdfs_stat(fs_, filename_.c_str());
    if(stat == NULL) {
        throw std::runtime_error("error in webhdfs_stat");
    }
    base::FileStatus s;
    s.blockSize = stat->block;
    s.length = stat->length;
    s.replicationFactor = stat->replication;
    webhdfs_fstat_free(stat);
    return s;


}

HdfsFile::~HdfsFile() {
    if(f_) {
        webhdfs_file_close(f_);
    }
    /* Disconnect from WebHDFS */
    if(fs_) {
        webhdfs_disconnect(fs_);
    }
    if(conf_) {
        webhdfs_conf_free(conf_);
    }
}

} // namespace hdfsutils
} // namespace ddc
