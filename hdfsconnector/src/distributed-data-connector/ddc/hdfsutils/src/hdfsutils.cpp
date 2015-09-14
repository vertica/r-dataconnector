#include "hdfsutils.h"

ddc::hdfsutils::HdfsGlobber::HdfsGlobber()
    : fs_(NULL),
      conf_(NULL),
      configured_(false) {

}

ddc::hdfsutils::HdfsGlobber::~HdfsGlobber() {
    /* Disconnect from WebHDFS */
    if(fs_) {
        webhdfs_disconnect(fs_);
    }
    if(conf_) {
        webhdfs_conf_free(conf_);
    }
}

void ddc::hdfsutils::HdfsGlobber::configure(base::ConfigurationMap &conf)
{
    std::string hdfsConfigurationFile;
    GET_PARAMETER(hdfsConfigurationFile, std::string, "hdfsConfigurationFile");

    char *error = NULL;
    conf_ = webhdfs_conf_load(hdfsConfigurationFile.c_str(), &error);
    if(!conf_) {
        std::ostringstream os;
        os << "Error reading hdfs config: " << hdfsConfigurationFile << " : ";
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

    configured_ = true;
}


bool ddc::hdfsutils::isHdfs(const std::string &protocol) {
    return protocol == "hdfs" || protocol == "http" || protocol == "https" || protocol == "webhdfs";
}


std::string ddc::hdfsutils::HdfsGlobber::getRoot(const std::string& path) {
    // find first special char (*, ? or [)
    size_t pos = path.find_first_of("*?[");
    if (pos == std::string::npos) {
        throw std::runtime_error("Doesn't contain *?[");
    }
    // move to the
    std::string s = path.substr(0, pos + 1);
    const char* root = dirname((char *)(s.c_str()));
    return std::string(root);
}

bool ddc::hdfsutils::HdfsGlobber::isFile(const std::string& path) {

    char *error = NULL;
    webhdfs_fstat *stat = webhdfs_stat(fs_, path.c_str(), &error);
    if (stat == NULL) {
        std::string errorStr(error);
        if (error) {
            free(error);
        }
        throw std::runtime_error(errorStr);
    }
    bool isFile = std::string(stat->type) != std::string("DIRECTORY");   //enum {FILE, DIRECTORY, SYMLINK}
    return isFile;
}

std::vector<std::string> ddc::hdfsutils::HdfsGlobber::listDir(const std::string& path) {
    if (isFile(path)) {
        LOG(ERROR) << path << " is not a dir";
        throw std::runtime_error("Needs to be dir");
    }

    const webhdfs_fstat_t *stat;
    webhdfs_dir_t *dir = webhdfs_dir_open(fs_, path.c_str());
    std::vector<std::string> files;
    while ((stat = webhdfs_dir_read(dir)) != NULL) {
        files.push_back(stat->path);
    }
    return files;
}

void ddc::hdfsutils::HdfsGlobber::walk(const std::string& root, std::vector<std::string>& files) {
    if (isFile(root)) {
        files.push_back(root);
        return;
    }
    else {
        std::vector<std::string> filesInDir = listDir(root);
        std::string root2 = root;
        if (root[root.size()-1] != '/') {
            root2 += "/";
        }
        for (uint64_t i = 0; i < filesInDir.size(); ++i) {
            walk(root2 + filesInDir[i], files);
        }
    }
}


std::vector<std::string> ddc::hdfsutils::HdfsGlobber::glob(const std::string& pat) {
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    std::vector<std::string> matches;
    std::string root;
    try {
        root = getRoot(pat);
    }
    catch(...) {
        matches.push_back(pat);
        return matches;
    }

    std::vector<std::string> files;
    walk(root, files);
    for(uint64_t i = 0; i < files.size(); ++i) {
        if (fnmatch(pat.c_str(), files[i].c_str(), 0) == 0) {
            matches.push_back(files[i]);
        }
    }
    return matches;
}


