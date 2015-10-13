#include "hdfsblocklocator.h"

#include <string.h>
#include "webhdfs/webhdfs_p.h"
#include <stdexcept>
#include <glog/logging.h>

namespace ddc {
namespace hdfsutils {



HdfsBlockLocator::HdfsBlockLocator()
    : fs_(NULL),
      conf_(NULL),
      configured_(false)
{    
}

HdfsBlockLocator::~HdfsBlockLocator()
{
    if(fs_) {
        webhdfs_disconnect(fs_);
    }
    if(conf_) {
        webhdfs_conf_free(conf_);
    }
}

void HdfsBlockLocator::configure(base::ConfigurationMap &conf) {
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");
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

    GET_PARAMETER(filename_, std::string, "filename");
    GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");

    GET_PARAMETER(fileStatCache_, boost::shared_ptr<base::Cache>, "fileStatCache");

    configured_ = true;
}

std::string HdfsBlockLocator::getHdfsBlockJson(const std::string &path) {
    yajl_val root, v;
    webhdfs_req_t req;

    webhdfs_req_open(&req, fs_, path.c_str());
    webhdfs_req_set_args(&req, "op=GET_BLOCK_LOCATIONS");
    char *error = NULL;
    webhdfs_req_exec(&req, WEBHDFS_REQ_GET, &error);
    if (error) free(error);
//    if(req.rcode != 200) { //TODO magic number
//        throw std::runtime_error("error in webhdfs_req_exec");
//    }
    if((root = webhdfs_req_json_response(&req)) == 0) {
        throw std::runtime_error("error in webhdfs_req_json_response");
    }

    //check for webhdfs exception
    if ((v = webhdfs_response_exception(root)) != NULL) {
        // get exception message
        const char *messageNode[] = {"message", NULL};
        yajl_val message;
        if ((message = yajl_tree_get(v, messageNode, yajl_t_string)) == NULL) {
            yajl_tree_free(root);
            throw std::runtime_error("Error parsing JSON exception");
        }

        std::string errorStr(YAJL_GET_STRING(message));
        yajl_tree_free(root);
        throw std::runtime_error(errorStr);
    }

    std::string json = std::string((const char*)req.buffer.blob, req.buffer.size);


    webhdfs_req_close(&req);
    yajl_tree_free(root);
    return json;
}

std::vector<HdfsBlock> HdfsBlockLocator::parseJson(const std::string& json) {
    std::vector<HdfsBlock> blocks;
    char errbuf[1024];
    errbuf[1023] = '\0';
    /* we have the whole config file in memory.  let's parse it ... */
    yajl_val node = yajl_tree_parse(json.c_str(), errbuf, sizeof(errbuf));

    /* parse error handling */
    if (node == NULL) {
        throw std::runtime_error(errbuf);
    }

    /* ... and extract a nested value from the config file */
    const char * located_blocks_path[] = { "LocatedBlocks", "locatedBlocks", (const char *) 0 };
    yajl_val located_blocks = yajl_tree_get(node, located_blocks_path, yajl_t_array);
    if(located_blocks) {
        for(uint64_t i = 0; i < located_blocks->u.array.len; i++) {
            yajl_val located_block = located_blocks->u.array.values[i];
            HdfsBlock hdfsBlock;
            if(located_block) {
                //get block info
                const char * block_path[] = { "block", (const char *) 0 };
                yajl_val block = yajl_tree_get(located_block, block_path, yajl_t_object);
                if(block) {
                    const char * block_id_path[] = { "blockId", (const char *) 0 };
                    yajl_val block_id = yajl_tree_get(block, block_id_path, yajl_t_number);
                    if(block_id) {
                        hdfsBlock.blockId = YAJL_GET_INTEGER(block_id);
                    }
                    const char * num_bytes_path[] = { "numBytes", (const char *) 0 };
                    yajl_val num_bytes = yajl_tree_get(block, num_bytes_path, yajl_t_number);
                    if(num_bytes) {
                        hdfsBlock.numBytes = YAJL_GET_INTEGER(num_bytes);
                    }
                }
                //get startOffset
                const char * start_offset_path[] = { "startOffset", (const char *) 0 };
                yajl_val start_offset = yajl_tree_get(located_block, start_offset_path, yajl_t_number);
                if(start_offset) {
                    hdfsBlock.startOffset = YAJL_GET_INTEGER(start_offset);
                }
                //get locations
                const char * locations_path[] = { "locations", (const char *) 0 };
                yajl_val locations = yajl_tree_get(located_block, locations_path, yajl_t_array);
                if(locations) {
                    hdfsBlock.locations.reserve(locations->u.array.len);
                    for(uint64_t i = 0; i < locations->u.array.len; i++) {
                        yajl_val location = locations->u.array.values[i];
                        if(location) {
                            const char * ip_addr_path[] = { "ipAddr", (const char *) 0 };
                            yajl_val ip_addr = yajl_tree_get(location, ip_addr_path, yajl_t_string);
                            if(ip_addr) {
                                //hdfsBlock.locations[0] = string(YAJL_GET_STRING(ip_addr));
                                hdfsBlock.locations.push_back(std::string(YAJL_GET_STRING(ip_addr)));
                            }
                        }
                    }
                }

            }
            blocks.push_back(hdfsBlock);
        }
    }

    yajl_tree_free(node);

    return blocks;
}


std::vector<HdfsBlock> HdfsBlockLocator::getHdfsBlocks(const std::string &path){
    if(!configured_) {
        throw std::runtime_error("Need to configure first");
    }
    std::string key = std::string("blockLocationJson_") + path;
    if (!fileStatCache_->contains(key)) {
        fileStatCache_->set(key, getHdfsBlockJson(path));
    }
    std::string json = boost::any_cast<std::string>(fileStatCache_->get(key));
    return parseJson(json);
}


void HdfsBlockLocator::findHdfsBlocks(const std::vector<hdfsutils::HdfsBlock>& hdfsBlocks,
                                     const uint64_t blockStart,
                                     const uint64_t numBytes,
                                     std::vector<hdfsutils::HdfsBlockRange>& blocks) {
    //DLOG(INFO) << "finding blocks, start: " << blockStart << " end: " << blockStart + numBytes << " bytes: " << numBytes;
    for(uint64_t i = 0; i < hdfsBlocks.size(); i++) {
        uint64_t hdfsStart = hdfsBlocks[i].startOffset;
        uint64_t hdfsEnd = hdfsBlocks[i].startOffset + hdfsBlocks[i].numBytes;
        if((blockStart >= hdfsStart ) && (blockStart < hdfsEnd)) {
            //intersects with current block
            hdfsutils::HdfsBlockRange blockRange;
            blockRange.block = hdfsBlocks[i];
            blockRange.range.start = blockStart;
            blockRange.range.end = std::min(blockStart + numBytes, hdfsEnd);
            blocks.push_back(blockRange);
//            DLOG(INFO) << "adding block " << hdfsBlocks_[i].blockId <<
//                          "start: " << blockRange.range.start << " end: " << blockRange.range.end;
            if((blockStart + numBytes) <= hdfsEnd) {
                //DLOG(INFO) << "we're done";
                return; //the whole block is in this hdfs block, we're done
            }
            else {
                //recurse to find subsequent blocks
                return findHdfsBlocks(hdfsBlocks,
                                      hdfsEnd,
                                      blockStart + numBytes - hdfsEnd,
                                      blocks);
            }
        }
    }
    throw std::runtime_error("unable to find hdfs blocks");
}

BufferPtr HdfsBlockLocator::getBlock(const uint64_t blockStart, const uint64_t numBytes) {
    /*
         * 1. Split in hdfs blocks
         * 2. Get datanodes that have that block
         * 3. create url
         * 4. download url
         */
    std::vector<HdfsBlockRange> blocks;
    std::vector<hdfsutils::HdfsBlock> hdfsBlocks = getHdfsBlocks(filename_);
    HdfsBlockLocator::findHdfsBlocks(hdfsBlocks, blockStart, numBytes, blocks);

    BufferPtr buffer = BufferPtr(new std::vector<uint8_t>);
    buffer->reserve(numBytes);

    base::ConfigurationMap blockLocatorConf;
    blockLocatorConf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    blockLocatorConf["fileStatCache"] = fileStatCache_;
    HdfsFile file(filename_);
    file.configure(blockLocatorConf);
    base::FileStatus s = file.stat();
    uint64_t blockSize = s.blockSize;


    for(uint64_t i = 0; i < blocks.size(); i++) {
        std::vector<std::string> urls = getUrls(blocks[i]);
        BufferPtr blockBuffer(new std::vector<uint8_t>());
        blockBuffer->reserve(std::min(blockSize, numBytes));
        hdfsutils::FailoverUrlDownloader urlDownloader;
        try {
            urlDownloader.download(urls, blockBuffer);
        }
        catch(HttpException& e) {
            char err[1024];
            yajl_val node,v;
            const char *exceptionMessage = e.what();
            // parse JSON
            if ((node = yajl_tree_parse(exceptionMessage, err, sizeof(err))) == NULL) {
                throw std::runtime_error("Error parsing JSON exception");
            }
            // get exception node
            if ((v = webhdfs_response_exception(node)) == NULL){
                yajl_tree_free(node);
                throw std::runtime_error("Error parsing JSON exception");
            }
            // get exception message
            const char *messageNode[] = {"message", NULL};
            yajl_val message;
            if ((message = yajl_tree_get(v, messageNode, yajl_t_string)) == NULL) {
                yajl_tree_free(node);
                throw std::runtime_error("Error parsing JSON exception");
            }

            std::string errorStr(YAJL_GET_STRING(message));
            yajl_tree_free(node);
            throw std::runtime_error(errorStr);
        }
        buffer->insert(buffer->end(),blockBuffer->begin(), blockBuffer->end());
        DLOG(INFO) << "Inserted " << blockBuffer->size() <<
                      " bytes. New total size: " << buffer->size();
    }
    if (buffer->size() != numBytes) {
        std::ostringstream os;
        os << "Error in hdfsblocklocator::getBlock, requested: " <<
              numBytes << " bytes but returned: " << buffer->size();
        throw std::runtime_error(os.str());
    }

    return buffer;
}

std::vector<std::string> HdfsBlockLocator::sortDatanodes(std::vector<std::string> &datanodes) {
    std::vector<std::string> sortedDatanodes;
    std::vector<std::string> localDatanodes;
    std::vector<std::string> remoteDatanodes;

    std::map<std::string, bool> localIpAddresses = base::utils::ipAddresses();

    for(uint64_t i = 0; i < datanodes.size(); i++) {
        if(localIpAddresses[datanodes[i]]) {
            DLOG(INFO) << "found local datanode " << datanodes[i];
            localDatanodes.push_back(datanodes[i]);
        }
        // TODO
        // if workers don't have the chunks locally, ensure chunks
        // are retrieved from different datanodes, not always the same one

        else remoteDatanodes.push_back(datanodes[i]);
    }
    sortedDatanodes.insert(sortedDatanodes.end(),localDatanodes.begin(),localDatanodes.end());
    sortedDatanodes.insert(sortedDatanodes.end(),remoteDatanodes.begin(),remoteDatanodes.end());
    return sortedDatanodes;
}

std::vector<std::string> HdfsBlockLocator::getUrls(const HdfsBlockRange &block) {
    std::vector<std::string> datanodes = block.block.locations;
    std::vector<std::string> urls;

    std::vector<std::string> sortedDatanodes = sortDatanodes(datanodes);

    hdfsutils::HdfsFile file(filename_);
    base::ConfigurationMap hdfsconf;
    hdfsconf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
    hdfsconf["fileStatCache"] = fileStatCache_;
    file.configure(hdfsconf);
    webhdfs_conf_t *conf = file.conf();
    if(!conf) {
        throw std::runtime_error("conf is null");
    }
    std::string namenodeIpAddr = std::string(conf->hdfs_host);
    for(uint64_t i = 0; i < sortedDatanodes.size(); i++) {
        //http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN[&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]
        std::string url = "http://" + sortedDatanodes[i] + ":50075/webhdfs/v1" + filename_ +
                "?op=OPEN&namenoderpcaddress=" + namenodeIpAddr + ":" + base::utils::to_string(conf->hdfs_port) +
                "&user.name=" + std::string(conf->hdfs_user) + "&offset=" +
                base::utils::to_string(block.range.start) + "&length=" + base::utils::to_string(block.range.end - block.range.start);
        urls.push_back(url);
    }
    return urls;
}

} // namespace hdfsutils
} // namespace ddc
