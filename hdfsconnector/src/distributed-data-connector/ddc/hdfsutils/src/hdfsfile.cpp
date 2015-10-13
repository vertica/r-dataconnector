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
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");

    try {
        overwrite_ = boost::any_cast<bool>(conf["overwrite"]);
    }
    catch(...) {
        // PASS
        //LOG(INFO) << "overwrite not specified. Defaulting to false";
    }

    /* Setup webhdfs config */
    DLOG(INFO) << "Reading HDFS config: " << hdfsConfigurationFile_;

    char *error = NULL;
    conf_ = webhdfs_conf_load(hdfsConfigurationFile_.c_str(), &error);
    if(!conf_) {
        std::ostringstream os;
        os << "Error reading hdfs config: " << hdfsConfigurationFile_ << " : ";
        std::string errorStr(os.str());
        if (error) {
            errorStr += std::string(error);
            free(error);
        }
        throw std::runtime_error(errorStr);
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

    GET_PARAMETER(fileStatCache_, boost::shared_ptr<base::Cache>, "fileStatCache");

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
    char *error = NULL;
    if (res = webhdfs_file_create(fs_, filename_.c_str(), overwrite_, __upload_func, &d, &error)) {
        std::ostringstream os;
        os << "Error writing file: " << std::string(error);
        if (error) {
            free(error);
            error = NULL;
        }
        throw std::runtime_error(os.str());
    }
    if (error) {
        free(error);
        error = NULL;
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

    if (!fileStatCache_->contains(filename_)) {
        char *error = NULL;
        webhdfs_fstat_t *stat = webhdfs_stat(fs_, filename_.c_str(), &error);
        if(stat == NULL) {
            std::string errorStr(error);
            if (error) {
                free(error);
            }
            throw std::runtime_error(errorStr);
        }
        base::FileStatus s;
        s.blockSize = stat->block;
        s.length = stat->length;
        s.replicationFactor = stat->replication;
        webhdfs_fstat_free(stat);

        fileStatCache_->set(filename_, s);
    }

    return boost::any_cast<base::FileStatus>(fileStatCache_->get(filename_));
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
