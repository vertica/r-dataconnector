#include "ddc.h"

#include <csignal>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <glog/logging.h>
#include <Rcpp.h>

#include "assembler/assemblerfactory.h"
#include "assembler/rdataframeassembler.h"
#include "blockreader/blockreaderfactory.h"
#include "ddc/globals.h"
#include "hdfsutils/filefactory.h"
#include "base/utils.h"
#include "base/ifile.h"
#include "recordparser/recordparserfactory.h"
#include "splitproducer/splitproducerfactory.h"

namespace ddc {

void intHandler(int dummy=0) {
    LOG(INFO) << "User cancelled operation. Exiting cleanly ...";
    stopDdc = true;
}


std::string schema2string(const CsvSchema& schema) {
    std::string schemaStr;
    for (CsvSchemaConstIt it = schema.begin(); it != schema.end(); ++it) {
        std::string colName = it->second.first;
        std::string colType = it->second.second;
        schemaStr += (colName + ":" + colType + ",");
    }
    return schemaStr.substr(0, schemaStr.size() - 1);
}

CsvSchema parseSchema(const std::string& schema) {
    //TODO implement better schemas like this one
    /*
     * EXAMPLE 1: Simple CSV Schema
     * version 1.0
     * @totalColumns 3
     * name: notEmpty
     * age: range(0, 120)
     * gender: is("m") or is("f") or is("t") or is("n")
     */

    CsvSchema res;

    std::vector<std::string> strs;
    //age:int32_t,name:string --> [age:int32_t, name:string]
    boost::split(strs, schema, boost::is_any_of(","));

    //TODO clean strings (spaces ...)
    for(u_int64_t i = 0; i < strs.size(); i++) {
        std::vector<std::string> column;
        //age:int32_t --> [age, int32_t]
        boost::split(column, strs[i], boost::is_any_of(":"));
        if(column.size() != 2) {
            throw std::runtime_error("error parsing schema");
        }
        res[i] = std::make_pair(column[0], column[1]);
    }
    return res;
}


std::vector<std::string> schema2colnames(const std::string &schema) {
    CsvSchema sc = parseSchema(schema);
    std::vector<std::string> res;
    for (CsvSchemaIt it = sc.begin(); it != sc.end(); ++it) {
        std::string colName = it->second.first;
        //std::string colType = it->second.second;
        res.push_back(colName);
    }
    return res;
}

std::vector<std::string> orccolnames(const std::string& url,
                                     const std::string& hdfsConfigurationFile) {
    base::ConfigurationMap conf;
    conf["url"] = url;
    conf["hdfsConfigurationFile"] = hdfsConfigurationFile;
    std::vector<uint64_t> stripes;
    conf["selectedStripes"] = stripes;
    recordparser::IRecordParserPtr p = boost::shared_ptr<recordparser::IRecordParser>(new recordparser::OrcRecordParser());
    p->configure(conf);

    assembler::RDataFrameAssembler a;
    base::ConfigurationMap conf2;
    conf2["url"] = url;
    conf2["hdfsConfigurationFile"] = hdfsConfigurationFile;
    conf2["format"] = std::string("row");
    conf2["recordParser"] = p;
    a.configure(conf2);

    std::vector<std::string> res = a.columnNames();
    return res;
}


void ddc_write(const std::string& url,
               const std::string& bytes,
               base::ConfigurationMap& conf) {
    std::string protocol = base::utils::getProtocol(url);
    std::string filename = base::utils::stripProtocol(url);
    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename, "w");
    if (hdfsutils::isHdfs(protocol)) {
        hdfsutils::HdfsFile *ptr = (hdfsutils::HdfsFile *)file.get();
        ptr->configure(conf);
    }
    size_t bytesWritten = file->write((void *)bytes.data(), bytes.length());  // will throw on error
}

// TODO Refactor this as offsets should be read from the file's metadata
// and only for files that make sense (not csv)
boost::any ddc_read(const std::string &url,
                    const std::string& objectType,
                    base::ConfigurationMap& conf)
{
    // register signal handlers
    signal(SIGINT, intHandler);
    signal(SIGKILL, intHandler);

    base::ConfigurationMap assemblerConf;
    std::string extension;
    base::ConfigurationMap::iterator it = conf.find("fileType");
    if (it != conf.end()) {
        extension = boost::any_cast<std::string>(it->second);
        assemblerConf["fileType"] = extension;
    }
    else {
        extension = base::utils::getExtension(url);
    }

    std::string protocol = base::utils::getProtocol(url);
    std::string filename = base::utils::stripProtocol(url);

    DLOG(INFO) << "url: " << url << " protocol: " << protocol <<
                  " filename: " << filename << " ext: " << extension << " objtype: " << objectType;

    assembler::IAssemblerPtr assembler = assembler::AssemblerFactory::makeAssembler(objectType);
    recordparser::IRecordParserPtr recordParser = recordparser::RecordParserFactory::makeRecordParser(extension);
    splitproducer::ISplitProducerPtr splitProducer;
    splitProducer = splitproducer::SplitProducerFactory::makeSplitProducer(extension);

    blockreader::IBlockReaderPtr blockReader = blockreader::BlockReaderFactory::makeBlockReader(protocol);

    base::IFilePtr file = hdfsutils::FileFactory::makeFile(protocol, filename, "r");
    if( hdfsutils::isHdfs(protocol)) {
        hdfsutils::HdfsFile *p = (hdfsutils::HdfsFile *)file.get();
        base::ConfigurationMap hdfsconf;
        hdfsconf["hdfsConfigurationFile"] = conf["hdfsConfigurationFile"];
        p->configure(hdfsconf);
    }
    base::FileStatus status = file->stat();

    base::ConfigurationMap blockReaderConf;
    blockReaderConf["filename"] = filename;
    blockReaderConf["blocksize"] = static_cast<uint64_t>(32 * 1024 * 1024); //TODO camelcase, only makes sense for local
    blockReaderConf["hdfsConfigurationFile"] = conf["hdfsConfigurationFile"];
    blockReader->configure(blockReaderConf);

    uint64_t chunkStart = 0;
    try {
        chunkStart = boost::any_cast<uint64_t>(conf["chunkStart"]);
    }
    catch(...) {
#if 0
        if (extension == "csv") {
            throw std::runtime_error("Need to specify chunkStart for CSV files");
        }
#endif
        if (extension == "csv") {
            // chunkStart default is 0
            LOG(INFO) << "chunkStart unspecified. Defaulting to 0.";
        }
    }

    uint64_t chunkEnd = 0;
    try {
        chunkEnd = boost::any_cast<uint64_t>(conf["chunkEnd"]);
        if (chunkEnd == -1) {
            LOG(INFO) << "chunkEnd unspecified. Defaulting to file size.";
            chunkEnd = status.length;
        }
    }
    catch(...) {
#if 0
        if(extension == "csv") {
            throw std::runtime_error("Need to specify chunkEnd for CSV files");
        }
#endif
        if (extension == "csv") {
            // chunkEnd default in fileSize
            LOG(INFO) << "chunkEnd unspecified. Defaulting to file size.";
            chunkEnd = status.length;
        }
    }

    std::vector<uint64_t> offsets;
    try {
        offsets = boost::any_cast<std::vector<uint64_t> >(conf["offsets"]);
    }
    catch(...) {
        //PASS
    }
    std::string schemaUrl = "";
    try {
        schemaUrl = boost::any_cast<std::string>(conf["schemaUrl"]);
    }
    catch(...) {
        if(extension == "csv") {
            throw std::runtime_error("Need to specify schema for CSV files");
        }
    }
    char delimiter = ',';
    try {
        delimiter = boost::any_cast<char>(conf["delimiter"]);
    }
    catch(...) {

    }

    char commentCharacter = '#';
    try {
        commentCharacter = boost::any_cast<char>(conf["commentCharacter"]);
    }
    catch(...) {

    }

    std::vector<uint64_t> selectedStripes;
    try {
        selectedStripes = boost::any_cast<std::vector<uint64_t> >(conf["selectedStripes"]);
    }
    catch(...) {
#if 0
        if(extension == "orc") {
            throw std::runtime_error("Need to specify selectedStripes for ORC files");
        }
#endif
        // if selectedStripes is empty we'll read all the stripes
    }

    base::ConfigurationMap splitProducerConf;
    if(extension == "csv") { //delimiter based splitters
        splitProducerConf["splitStart"] = static_cast<uint64_t>(chunkStart);
        splitProducerConf["splitEnd"] = static_cast<uint64_t>(chunkEnd);
        splitProducerConf["delimiter"] = static_cast<uint8_t>('\n'); //TODO make this configurable
    }
    else if(extension == "offsetcsv") { //offset based splitters
        if(offsets.size() == 0) {
            throw std::runtime_error("need to configure offsets");
        }
        splitProducerConf["offsets"] = offsets;

    }
    //common
    splitProducerConf["blockReader"] = static_cast<blockreader::IBlockReaderPtr>(blockReader);
    splitProducerConf["fileEnd"] = static_cast<uint64_t>(status.length);
    splitProducer->configure(splitProducerConf);

    base::ConfigurationMap recordParserConf;
    recordParserConf["splitProducer"] = splitProducer;
    recordParserConf["delimiter"] = delimiter;
    recordParserConf["commentCharacter"] = commentCharacter;
    //TODO read schema file from storage
    if((extension == "csv" || extension == "offsetcsv") && schemaUrl == "") {
        throw std::runtime_error("need to specify schema to load csv file");
    }
    std::map<int32_t, std::pair<std::string,std::string> >schema;
    if((extension == "csv" || extension == "offsetcsv")) {
        schema = parseSchema(schemaUrl);
        recordParserConf["schema"] = schema;
    }
    else if(extension == "orc") {
        recordParserConf["url"] = url;
        recordParserConf["selectedStripes"] = selectedStripes;
    }

    recordParserConf["hdfsConfigurationFile"] = conf["hdfsConfigurationFile"];
    recordParser->configure(recordParserConf);

    std::string format = "row"; //default to row
    if((extension == "parquet") || (extension == "orc")) {
        format = "column";
    }

    assemblerConf["recordParser"] = recordParser;
    assemblerConf["format"] = format;
    assemblerConf["schema"] = schema;
    assemblerConf["url"] = url;
    assemblerConf["hdfsConfigurationFile"] = conf["hdfsConfigurationFile"];
    assembler->configure(assemblerConf);

    if (extension == "csv") {
        DLOG(INFO) << "Schema: " << schemaUrl <<
                      " delimiter: " << delimiter;
    }
    else if (extension == "orc") {
//        DLOG(INFO) << "Selected stripes: " << selectedStripes;
    }

    return assembler->getObject();

}

Rcpp::List create_plan(const std::string& url,
                base::ConfigurationMap& options,
                const ddc::scheduler::WorkerMap& workerMap) {

    std::string extension = base::utils::getExtension(url);
    std::string protocol = base::utils::getProtocol(url);

    base::ConfigurationMap conf;

    /**
     * Put together the configuration needed by the chunk scheduler.
     */
    conf["workerMap"] = workerMap;
    conf["fileUrl"] = url;
    conf["hdfsBlockLocator"] = ddc::hdfsutils::HdfsBlockLocatorPtr(new ddc::hdfsutils::HdfsBlockLocator());

    base::ConfigurationMap conf2;
    std::string schema = "";
    try {
        schema =  boost::any_cast<std::string>(options["schema"]);
        conf2["schema"] = schema;
    }
    catch(...) {
        //PASS
    }

    conf["options"] = conf2;

    try {
        std::string hdfsConfigurationFile = boost::any_cast<std::string>(options["hdfsConfigurationFile"]);
        conf["hdfsConfigurationFile"] = hdfsConfigurationFile;
    }
    catch(...) {
        //PASS
        if (protocol == "http" ||
            protocol == "hdfs" ||
            protocol == "webhdfs") {
            throw std::runtime_error("Need to specify hdfsConfigurationFile in options");
        }
    }

    try {
        std::string fileType = boost::any_cast<std::string>(options["fileType"]);
        conf["fileType"] = fileType;
        extension = fileType;
    }
    catch(...) {
        //PASS
        // If user doesn't specify filetype we deduce it from the file extension
    }

    std::string delimiter = ",";
    try {
        delimiter = boost::any_cast<char>(options["delimiter"]);
    }
    catch(...) {
        //PASS
    }

    std::string commentCharacter = "#";
    try {
        commentCharacter = boost::any_cast<char>(options["commentCharacter"]);
    }
    catch(...) {
        //PASS
    }

    scheduler::ChunkScheduler chunkScheduler;
    chunkScheduler.configure(conf);

    /**
     * Create a plan that contains information on how to schedule the file
     * load across executors.
     */
    ddc::scheduler::Plan plan = chunkScheduler.schedule();

    /**
     * Prepare output object (res) with all the relevant info.
     */
    Rcpp::List res;
    res["num_partitions"] = plan.numSplits;

    Rcpp::List configs;
    for(int i = 0; i < plan.configurations.size(); ++i) {
        base::ConfigurationMap planconf = plan.configurations[i];
        Rcpp::List rconf;
        if(extension == "csv") {
            try {
                rconf["chunk_start"] = boost::any_cast<unsigned long>(planconf["chunkStart"]);
                rconf["chunk_end"] = boost::any_cast<unsigned long>(planconf["chunkEnd"]);
                rconf["schema"] = boost::any_cast<std::string>(planconf["schema"]);
                rconf["file_type"] = std::string("csv");
                rconf["delimiter"] = delimiter;
                rconf["comment_character"] = commentCharacter;
                rconf["url"] = boost::any_cast<std::string>(planconf["url"]);
            }
            catch(...) {
                throw std::runtime_error("Plan doesn't contain all required information");
            }
        }
        else if(extension == "orc") {
            try {
                rconf["selected_stripes"] = boost::any_cast<std::string>(planconf["selectedStripes"]);
                rconf["file_type"] = std::string("orc");
                rconf["url"] = boost::any_cast<std::string>(planconf["url"]);
            }
            catch(...) {
                throw std::runtime_error("Plan doesn't contain all required information");
            }
        }
        else {
            throw std::runtime_error("Unsupported file format or cannot detect extension");
        }
        configs.push_back(rconf);
    }
    res["configs"] = configs;
    ddc::scheduler::ChunkWorkerMap chunkWorkerMap =
            chunkScheduler.chunkWorkerMap();
    Rcpp::List chunkWorkerMapList;
    for (uint64_t i = 0; i < chunkWorkerMap.size(); ++i) {
        chunkWorkerMapList[base::utils::to_string(i)] = chunkWorkerMap[i];
    }
    res["chunk_worker_map"] = chunkWorkerMapList;
    return res;
}

} // namespace ddc


