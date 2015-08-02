#include <stdint.h>

#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
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
        throw std::runtime_error("Unsupported R object. Must be string or raw");
        break;
    }

    return R_NilValue;
}

// [[Rcpp::export]]
Rcpp::DataFrame ddc_read(const std::string& url,
                         const Rcpp::List& options)
{
    base::ConfigurationMap conf;
    //std::vector<uint64_t> offsets;
    std::string schema = "";
    try {
        schema =  Rcpp::as<std::string>(options["schema"]);
        conf["schemaUrl"] = schema;
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

    boost::any df = ddc::ddc_read(url,
                                  "rdataframe",
                                  conf);
    return *(boost::any_cast<boost::shared_ptr<Rcpp::DataFrame> >(df));
}


