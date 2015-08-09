#include <stdint.h>

#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <Rcpp.h>

namespace base {
namespace utils {
std::string getExtension(const std::string &file);
}
}

namespace base {
typedef std::map<std::string, boost::any> ConfigurationMap;
} // namespace base

namespace ddc {
boost::any ddc_read(const std::string& url,
                    const std::string& objectType,
                    base::ConfigurationMap& conf);
void ddc_write(const std::string& url,
               const std::string& bytes,
               base::ConfigurationMap& conf);

namespace scheduler {

    class WorkerInfo {
    public:
        explicit WorkerInfo(const std::string& hostname,
                            const uint64_t port,
                            const uint64_t numExecutors)
            : hostname_(hostname),
              port_(port),
              numExecutors_(numExecutors) {
        }

        std::string hostname() const {
            return hostname_;
        }
        uint64_t port() const {
            return port_;
        }
        uint64_t numExecutors() const {
            return numExecutors_;
        }

    private:
        std::string hostname_;
        uint64_t port_;
        uint64_t numExecutors_;
    };

    typedef std::map<int32_t, boost::shared_ptr<WorkerInfo> > WorkerMap;
}

Rcpp::List create_plan(const std::string& url,
                       base::ConfigurationMap& options,
                       const ddc::scheduler::WorkerMap& workerMap);

}


// [[Rcpp::export]]
SEXP ddc_write(SEXP object,
               SEXP url,
               const Rcpp::List& options) {
    base::ConfigurationMap conf;
    std::string hdfsConfigurationFile;
    try {
        hdfsConfigurationFile =  Rcpp::as<std::string>(options["hdfsConfigurationFile"]);
        conf["hdfsConfigurationFile"] = hdfsConfigurationFile;
    }
    catch(...) {
        //PASS
    }
    bool overwrite = false;
    try {
        overwrite =  Rcpp::as<bool>(options["overwrite"]);
        conf["overwrite"] = overwrite;
    }
    catch(...) {
        //PASS
    }

    switch(TYPEOF(object)) {
    case STRSXP: {
        ddc::ddc_write(CHAR(STRING_ELT(url, 0)), CHAR(STRING_ELT(object, 0)), conf);
        break;
    }
    case RAWSXP: {
        std::string buffer;
        buffer.resize(Rf_length(object));
        memcpy((void *)buffer.data(), RAW(object), Rf_length(object));
        ddc::ddc_write(CHAR(STRING_ELT(url, 0)), buffer, conf);
        break;
    }
    default:
        throw std::runtime_error("Unsupported R object. Must be string or raw.");
        break;
    }

    return R_NilValue;
}

static base::ConfigurationMap list2conf(const Rcpp::List& options) {
    base::ConfigurationMap conf;
    //std::vector<uint64_t> offsets;
    std::string schema = "";
    try {
        schema =  Rcpp::as<std::string>(options["schema"]);
        conf["schema"] = schema;
        conf["schemaUrl"] = schema;  //TODO only 1 should be necessary
    }
    catch(...) {
        //PASS
    }

    uint64_t chunkStart = 0;
    try {
        chunkStart =  Rcpp::as<uint64_t>(options["chunkStart"]);
        conf["chunkStart"] = chunkStart;
    }
    catch(...) {
        //PASS
    }

    uint64_t chunkEnd = 0;
    try {
        chunkEnd =  Rcpp::as<uint64_t>(options["chunkEnd"]);
        conf["chunkEnd"] = chunkEnd;
    }
    catch(...) {
        //PASS
    }

    char delimiter = ',';
    try {
        delimiter =  Rcpp::as<char>(options["delimiter"]);
    }
    catch(...) {
        //PASS
    }
    conf["delimiter"] = delimiter;

    char commentCharacter = '#';
    try {
        commentCharacter =  Rcpp::as<char>(options["commentCharacter"]);
    }
    catch(...) {
        //PASS
    }
    conf["commentCharacter"] = commentCharacter;

    std::string selectedStripesStr = "";
    try {
        // TODO do this in ddc.cpp
        selectedStripesStr =  Rcpp::as<std::string>(options["selectedStripes"]);
        std::vector<std::string> selectedStripesStrings;
        boost::split(selectedStripesStrings, selectedStripesStr, boost::is_any_of(","));
        std::vector<uint64_t> selectedStripes;
        for(int i = 0; i < selectedStripesStrings.size(); ++i) {
            selectedStripes.push_back((uint64_t)atoll(selectedStripesStrings[i].c_str()));
        }
        conf["selectedStripes"] = selectedStripes;
    }
    catch(...) {
        //PASS
    }

    std::string hdfsConfigurationFile;
    try {
        hdfsConfigurationFile =  Rcpp::as<std::string>(options["hdfsConfigurationFile"]);
        conf["hdfsConfigurationFile"] = hdfsConfigurationFile;
    }
    catch(...) {
        //PASS
    }

    std::string fileType = "";
    try {
        fileType =  Rcpp::as<std::string>(options["fileType"]);
        conf["fileType"] = fileType;
    }
    catch(...) {
        //PASS
    }
    return conf;
}

// [[Rcpp::export]]
Rcpp::DataFrame ddc_read(const std::string& url,
                         const Rcpp::List& options)
{
    base::ConfigurationMap conf = list2conf(options);

    boost::any df = ddc::ddc_read(url,
                                  "rdataframe",
                                  conf);
    return *(boost::any_cast<boost::shared_ptr<Rcpp::DataFrame> >(df));
}

// [[Rcpp::export]]
Rcpp::List create_plan(const std::string& url,
                       const Rcpp::List& options,
                       const Rcpp::List& workerMapR) {

    base::ConfigurationMap conf = list2conf(options);

    // convert R workerMap into a c++ worker map
    ddc::scheduler::WorkerMap workerMap;
    for (uint64_t i = 0; i < workerMapR.size(); ++i) {
        std::ostringstream os;
        os << i;
        Rcpp::List w = workerMapR[os.str()];
        ddc::scheduler::WorkerInfo wi(w["hostname"],
                                      w["port"],
                                      w["num_executors"]);
        workerMap[i] = boost::make_shared<ddc::scheduler::WorkerInfo>(wi);
    }
    Rcpp::List res = ddc::create_plan(url, conf, workerMap);
    return res;
}

