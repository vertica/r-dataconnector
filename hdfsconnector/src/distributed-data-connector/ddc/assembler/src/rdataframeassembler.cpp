#include "rdataframeassembler.h"

namespace ddc {
namespace assembler {

RDataFrameAssembler::RDataFrameAssembler() :
    colIndex_(0),
    numRows_(0),
    numCols_(0),
    numTopCols_(0),
    totalRowsProcessed_(0),
    recordsAssembled_(0),
    splitsAssembled_(0),
    numColsInLastRow_(-1),
    configured_(false)
{
}

RDataFrameAssembler::~RDataFrameAssembler()
{
}

void RDataFrameAssembler::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(url_, std::string, "url");

    // Override extension
    base::ConfigurationMap::iterator it = conf.find("fileType");
    if (it != conf.end()) {
        extension_ = boost::any_cast<std::string>(it->second);
    }
    else {
        extension_ = base::utils::getExtension(url_);
    }

    std::string protocol = base::utils::getProtocol(url_);
    if (hdfsutils::isHdfs(protocol)) {
        GET_PARAMETER(hdfsConfigurationFile_, std::string, "hdfsConfigurationFile");
    }

    GET_PARAMETER(recordParser_, recordparser::IRecordParserPtr, "recordParser");

    if(extension_ == "orc") {
        configureOrc();
    }
    else {
        // we use this for CSV, but not for ORC
        #define DDC_COMMA ,
        GET_PARAMETER(schema_, std::map<int32_t DDC_COMMA std::pair<std::string DDC_COMMA std::string> > , "schema");
        #undef DDC_COMMA
    }

    GET_PARAMETER(format_, std::string, "format");
    if(format_ != "row" && format_ != "column") {
        throw std::runtime_error("format needs to be row or column");
    }
    configured_ = true;
}

void RDataFrameAssembler::configureOrc()
{
    std::string protocol = base::utils::getProtocol(url_);
    std::string filename = base::utils::stripProtocol(url_);

    std::unique_ptr<orc::Reader> orcReader;
    orc::ReaderOptions opts;

    // create ORC reader
    if (hdfsutils::isHdfs(protocol)) {
        hdfsutils::HdfsInputStream *p = new hdfsutils::HdfsInputStream(filename);
        base::ConfigurationMap hdfsconf;
        hdfsconf["hdfsConfigurationFile"] = hdfsConfigurationFile_;
        p->configure(hdfsconf);
        std::unique_ptr<orc::InputStream> inputStream(p);
        orcReader = orc::createReader(std::move(inputStream), opts);
    }
    else {
        orcReader = orc::createReader(orc::readLocalFile(filename), opts);
    }

    uint64_t numStripes = orcReader->getNumberOfStripes();
    if(numStripes > 0) {
        numCols_ = orcReader->getStripeStatistics(0)->getNumberOfColumns();
        std::unique_ptr<orc::StripeInformation> stripeInfo = orcReader->getStripe(0);
    }

    numRows_ = ((recordparser::OrcRecordParser *)recordParser_.get())->rows();

    orc::TypeKind kind = orcReader->getType().getKind();
    if(kind == orc::STRUCT) {
        std::unique_ptr<orc::ColumnVectorBatch> batch = orcReader->createRowBatch(numRows_);
        numTopCols_ = ((orc::StructVectorBatch*)(batch.get()))->fields.size();
        //create schema
        for(uint64_t i = 0; i < numTopCols_; ++i) {
            orc::TypeKind kind = orcReader->getType().getSubtype(i).getKind();
            std::string name = orcReader->getType().getFieldName(i);
            orcSchema_[i] = std::make_pair(name, kind);
        }
    }
    else {
        // In principle ORC files always have a top struct column
        LOG(WARNING) << "Top ORC column is not of STRUCT type";

        //create schema
        orc::TypeKind kind = orcReader->getType().getKind();
        std::string name = "0";
        orcSchema_[0] = std::make_pair(name, kind);
        numTopCols_ = 1;
    }

    for(uint64_t i = 0; i < numTopCols_; ++i) {
        rowIndexes_.push_back(0);  // initialize row index for each column
    }

    DLOG(INFO) << "cols: " << numCols_ <<
                  " topcols: " << numTopCols_ <<
                  " rows: " << numRows_;

    // TODO orcSchema_ is messed up when the top level column is not a struct

    // create columns
    uint64_t numSubTypes = orcReader->getType().getSubtypeCount();
    for (uint64_t i = 0; i < numTopCols_; ++i) {

        // if column has a name
        if(i < numSubTypes) {
            std::string colname = orcReader->getType().getFieldName(i);
            columnNames_.push_back(colname);
        }
        // else assign index j as column name
        else {
            columnNames_.push_back(base::utils::to_string(i));
        }

        switch((orcSchema_[i]).second) {
        case orc::BOOLEAN: {
            columnTypes_.push_back(ORC_BOOL_COL);
            columns_.push_back(BoolVectorPtr(new std::vector<bool>));
            break;
        }
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT: {
            columnTypes_.push_back(ORC_INT32_COL);
            columns_.push_back(Int32VectorPtr(new std::vector<int32_t>));
            break;
        }
        case orc::LONG: {
            columnTypes_.push_back(ORC_DOUBLE_COL);
            columns_.push_back(DoubleVectorPtr(new std::vector<double>));
            break;
        }
        case orc::FLOAT:
        case orc::DOUBLE:{
            columnTypes_.push_back(ORC_DOUBLE_COL);
            columns_.push_back(DoubleVectorPtr(new std::vector<double>));
            break;
        }
        case orc::BINARY:
        case orc::STRING:
        case orc::DECIMAL:
        case orc::TIMESTAMP:
        case orc::DATE:
        case orc::VARCHAR:
        case orc::CHAR: {
            columnTypes_.push_back(ORC_STRING_COL);
            columns_.push_back(CharacterVectorPtr(new std::vector<std::string>));
            break;
        }
        case orc::UNION:
        case orc::MAP:
        case orc::STRUCT:
        case orc::LIST: {
            columnTypes_.push_back(ORC_LIST_COL);
            SEXP col = PROTECT(Rf_allocVector(VECSXP, numRows_));
            columns_.push_back(col);
            break;
        }
        default : {
            throw std::runtime_error("unsupported orc col type");
        }
        }
    }
}

/**
 * @brief ParseValue
 * Called when processing a complex type (struct/list/map)
 * If the node is a primitive type (int, double, string ...) just returns.
 * Otherwise recurse.
 * @param node
 * @param level Recursion level
 * @return
 */

SEXP RDataFrameAssembler::ParseValue(recordparser::NodePtr& node, int level) {
    switch(node->type) {
        case orc::BOOLEAN: {
            DLOG(INFO) << "returning BOOL: " <<
                          boost::get<int64_t>(node->value) <<
                          " addr: " << &(node->value) <<
                          ", level: " <<  level;
            if (node->isNull) return Rf_ScalarLogical(NA_LOGICAL);
            else return Rf_ScalarLogical(static_cast<bool>(boost::get<int64_t>(node->value)));
        }
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT: {
            DLOG(INFO) << "returning INT: " <<
                      boost::get<int64_t>(node->value) <<
                          " addr: " << &(node->value) <<
                      ", level: " <<  level;
            if (node->isNull) return Rf_ScalarInteger(NA_INTEGER);
            else return Rf_ScalarInteger(static_cast<int32_t>(boost::get<int64_t>(node->value)));
        }
        case orc::LONG: {
            DLOG(INFO) << "returning LONG: " <<
                      boost::get<int64_t>(node->value) <<
                          " addr: " << &(node->value) <<
                      ", level: " <<  level;
            if (node->isNull) return Rf_ScalarReal(NA_REAL);
            else return Rf_ScalarReal(static_cast<double>(boost::get<int64_t>(node->value)));
        }
        case orc::FLOAT:
        case orc::DOUBLE:{
            DLOG(INFO) << "returning DOUBLE: " <<
                  boost::get<double>(node->value) <<
                          " addr: " << &(node->value) <<
                  ", level: " <<  level;
            if (node->isNull) return Rf_ScalarReal(NA_REAL);
            else return Rf_ScalarReal((double)(boost::get<double>(node->value)));
        }
        case orc::BINARY:
        case orc::STRING:
        case orc::DECIMAL:
        case orc::TIMESTAMP:
        case orc::DATE:
        case orc::VARCHAR:
        case orc::CHAR: {
            DLOG(INFO) << "returning STRING: " <<
                  boost::get<std::string>(node->value) <<
                          " addr: " << &(node->value) <<
                  ", level: " <<  level;
            if (node->isNull) return NA_STRING;
            else {
                SEXP tmp = PROTECT(Rf_allocVector(STRSXP, 1));
                SET_STRING_ELT(tmp, 0, Rf_mkCharCE(boost::get<std::string>(node->value).c_str(),  CE_UTF8));
                UNPROTECT(1);
                return tmp;
            }
        }
        case orc::MAP: {
            int len = node->kvpairs.size();
            DLOG(INFO) << "returning MAP of " << len << " elements, level " << level;

            Rcpp::DataFrame map;
            Rcpp::StringVector keys;
            Rcpp::List values;

            // for each K/V pair
            for (int i = 0; i < len; ++i) {
                std::string key = boost::get<std::string>(((node->kvpairs[i]).first)->value);
                keys.push_back(key);
                values.push_back(ParseValue((node->kvpairs[i]).second,level + 1));
            }

            map["key"] = keys;

            bool mapValueIsStruct = false;
            if(len > 0) {
                recordparser::NodePtr n = node->kvpairs[0].second;
                if(n->type == orc::STRUCT) {
                    mapValueIsStruct = true;
                }
            }

            Rcpp::StringVector rownames(len);
            for(uint64_t i = 0; i < len; ++i) {
                rownames[i] = base::utils::to_string(i+1);
            }
            if (mapValueIsStruct) {
                //
                // If the map value is a struct convert it to a DF
                //
                uint64_t numRows = values.size();
                Rcpp::List firstRow = values[0];
                uint64_t numCols = firstRow.size();
                Rcpp::List df;
                for(uint64_t i = 0; i < numCols; ++i) {
                    Rcpp::List col;
                    for(uint64_t j = 0; j < numRows; ++j) {
                        Rcpp::List l = values[j];
                        col.push_back(l[i]);
                    }
                    df.push_back(col, (node->kvpairs[0].second)->fieldNames[i]);
                }

                DLOG(INFO) << "MAP, Child is a struct. Transposing and making data frame " <<
                              " rows: " << numRows <<
                              " cols: " << numCols;


                df.attr("class") = "data.frame";
                df.attr("row.names") = rownames;
                //df.attr("names") = rownames;

                map["value"] = df;
            }
            else  {
                map["value"] = values;
            }

            // a map is a DF
            map.attr("row.names") = rownames;
            map.attr("class") = "data.frame";

            return map;
        }
        case orc::STRUCT:
        case orc::LIST: {
            int len = node->elements.size();
            if (node->type == orc::LIST) {
                DLOG(INFO) << "returning LIST of " << len << " elements, level " << level;
            }
            else {
                DLOG(INFO) << "returning STRUCT of " << len << " elements, level " << level;
            }
            Rcpp::List list;
            Rcpp::StringVector rownames(len);

            // for each list element
            for (int i = 0; i < len; ++i) {
                // TODO get real keys?
                list.push_back(ParseValue(node->elements[i],level + 1));
                rownames[i] = base::utils::to_string(i+1);
            }

            bool childIsStruct = false;
            if(len > 0) {
                recordparser::NodePtr n = node->elements[0];
                if(n->type == orc::STRUCT) {
                    childIsStruct = true;
                }
            }

            if (childIsStruct) {
                //
                // If child is a struct convert it to a DF
                //
                uint64_t numRows = list.size();
                Rcpp::List firstRow = list[0];
                uint64_t numCols = firstRow.size();
                Rcpp::List df;
                for(uint64_t i = 0; i < numCols; ++i) {
                    Rcpp::List col;
                    for(uint64_t j = 0; j < numRows; ++j) {
                        Rcpp::List l = list[j];
                        col.push_back(l[i]);
                    }
                    DLOG(INFO) << *node.get();
                    df.push_back(col, (node->elements[0])->fieldNames[i]);
                }

                DLOG(INFO) << "STRUCT/LIST, Child is a struct. Transposing and making data frame " <<
                              " rows: " << numRows <<
                              " cols: " << numCols;


                df.attr("class") = "data.frame";
                df.attr("row.names") = rownames;
                //df.attr("names") = rownames;

                return df;
            }
            else  {
                return list;
            }
        }
        default : {
            LOG(ERROR) << "unsupported";
            throw std::runtime_error("unsupported");
        }
    }
}

/**
 * Functions to create R dataframes and lists
 * TODO move to separate files?
 */
static SEXP createDataframe(const uint64_t ncols,
                            std::vector<SEXPTYPE> colTypes,
                            std::vector<std::string> colNames,
                            const uint64_t nrows
                            ) {
    SEXP ret, col, cls, nam, rownam;
    PROTECT(ret = Rf_allocVector(VECSXP, ncols)); // a list with two elements
    for(uint64_t i = 0; i < ncols; ++i) {
        PROTECT(col = Rf_allocVector(colTypes[i], nrows)); // first column
        SET_VECTOR_ELT(ret, i, col);
        UNPROTECT(1);
    }

    PROTECT(cls = Rf_allocVector(STRSXP, 1)); // class attribute
    SET_STRING_ELT(cls, 0, Rf_mkChar("data.frame"));
    Rf_classgets(ret, cls);
    UNPROTECT(1);

    PROTECT(nam = Rf_allocVector(STRSXP, ncols)); // names attribute (column names)
    for(uint64_t i = 0; i < ncols; ++i) {
        SET_STRING_ELT(nam, i, Rf_mkChar(colNames[i].c_str()));
    }
    Rf_namesgets(ret, nam);
    UNPROTECT(1);

    PROTECT(rownam = Rf_allocVector(STRSXP, nrows)); // row.names attribute
    for(uint64_t i = 0; i < nrows; ++i) {
        SET_STRING_ELT(rownam, i, Rf_mkChar(base::utils::to_string(i+1).c_str()));
    }
    Rf_setAttrib(ret, R_RowNamesSymbol, rownam);
    UNPROTECT(1);

    UNPROTECT(1);  // ret
    return ret;
}

static SEXP createEmptyList() {
    SEXP ret;
    PROTECT(ret = Rf_allocVector(VECSXP, 0));
    UNPROTECT(1);
    return ret;
}

static SEXP createEmptyDataframe() {
    std::vector<SEXPTYPE> a;
    std::vector<std::string> b;

    return createDataframe(0, a, b, 0);
}

void RDataFrameAssembler::handleOrcRecord(boost::any& record)
{    
    recordparser::NodePtr node = boost::any_cast<recordparser::NodePtr>(record);
    //recordparser::traverse(node);

    switch(node->type) {
        case orc::BOOLEAN:{
            if (node->isNull) {
                nullBools_[colIndex_].push_back((boost::get<BoolVectorPtr>(columns_[colIndex_]))->size());
                (boost::get<BoolVectorPtr>(columns_[colIndex_]))->push_back(NA_LOGICAL);
            }
            else {
                int64_t number = boost::get<int64_t>(node->value);
                (boost::get<BoolVectorPtr>(columns_[colIndex_]))->push_back(static_cast<bool>(number));
            }
            break;
        }
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT: {
            if (node->isNull) {
                (boost::get<Int32VectorPtr>(columns_[colIndex_]))->push_back(NA_INTEGER);
            }
            else {
                int64_t number = boost::get<int64_t>(node->value);
                (boost::get<Int32VectorPtr>(columns_[colIndex_]))->push_back(static_cast<int32_t>(number));
            }
            break;
        }
        case orc::LONG: {
            if (node->isNull) {
                (boost::get<DoubleVectorPtr>(columns_[colIndex_]))->push_back(NA_REAL);
            }
            else {
                int64_t number = boost::get<int64_t>(node->value);
                (boost::get<DoubleVectorPtr>(columns_[colIndex_]))->push_back(static_cast<double>(number));
            }
            break;
        }
        case orc::FLOAT:
        case orc::DOUBLE:{
            if (node->isNull) {
                (boost::get<DoubleVectorPtr>(columns_[colIndex_]))->push_back(NA_REAL);
            }
            else {
                double number = boost::get<double>(node->value);
                (boost::get<DoubleVectorPtr>(columns_[colIndex_]))->push_back(number);
            }
            break;
        }
        case orc::BINARY:
        case orc::STRING:
        case orc::DECIMAL:
        case orc::TIMESTAMP:
        case orc::DATE:
        case orc::VARCHAR:
        case orc::CHAR:
        {
            if (node->isNull) {
                nullStrings_[colIndex_].push_back((boost::get<CharacterVectorPtr>(columns_[colIndex_]))->size());
                (boost::get<CharacterVectorPtr>(columns_[colIndex_]))->push_back("NA");  // TODO replaced by real strings NAs
            }
            else {
                std::string& str = boost::get<std::string>(node->value);
                (boost::get<CharacterVectorPtr>(columns_[colIndex_]))->push_back(str);
            }
            break;
        }
        case orc::UNION: {
            // TODO unimplemented
            //LOG(WARNING) << "Union type is not implemented";
            SEXP val = R_NilValue;
            SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
            rowIndexes_[colIndex_] += 1;
            break;
        }
        case orc::MAP:{
            int len = node->kvpairs.size();
            if (len == 0 ||
                    node->isNull) {
                SEXP val = PROTECT(Rcpp::wrap(Rcpp::DataFrame()));
                SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
                UNPROTECT(1);
                rowIndexes_[colIndex_] += 1;
                break;
            }

            //TODO this assumes string keys
            Rcpp::DataFrame map;
            Rcpp::CharacterVector keys;
            Rcpp::List values;

            for(int i = 0; i < len; ++i) {
                std::string key = boost::get<std::string>(((node->kvpairs[i]).first)->value);
                keys.push_back(key);
                values.push_back(ParseValue((node->kvpairs[i]).second,0));
            }

            map["key"] = keys;

            bool mapValueIsStruct = false;
            if(len > 0) {
                recordparser::NodePtr n = node->kvpairs[0].second;
                if(n->type == orc::STRUCT) {
                    mapValueIsStruct = true;
                }
            }

            Rcpp::StringVector rownames(len);
            for(uint64_t i = 0; i < len; ++i) {
                rownames[i] = base::utils::to_string(i+1);
            }

            if (mapValueIsStruct) {
                //
                // If the map value is a struct convert it to a DF
                //
                uint64_t numRows = values.size();
                Rcpp::List firstRow = values[0];
                uint64_t numCols = firstRow.size();
                Rcpp::List df;
                for(uint64_t i = 0; i < numCols; ++i) {
                    Rcpp::List col;
                    for(uint64_t j = 0; j < numRows; ++j) {
                        Rcpp::List l = values[j];
                        col.push_back(l[i]);
                    }
                    DLOG(INFO) << *node.get();
                    df.push_back(col, ((node->kvpairs[0]).second)->fieldNames[i]);
                }

                DLOG(INFO) << "MAP, Child is a struct. Transposing and making data frame " <<
                              " rows: " << numRows <<
                              " cols: " << numCols;


                df.attr("class") = "data.frame";
                df.attr("row.names") = rownames;
                //df.attr("names") = rownames;

                map["value"] = df;
            }
            else  {
                map["value"] = values;
            }

            map.attr("row.names") = rownames;
            map.attr("class") = "data.frame";

            SEXP val = PROTECT(Rcpp::wrap(map));
            SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
            UNPROTECT(1);
            rowIndexes_[colIndex_] += 1;

            break;
        }

        case orc::STRUCT:
        case orc::LIST: {
            int len = node->elements.size();

            if(node->type == orc::STRUCT) {
                topStructColumnNames_[colIndex_] = node->fieldNames;
            }

            if (len == 0 ||
                    node->isNull) {
                SEXP val = PROTECT(Rcpp::wrap(Rcpp::List()));
                SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
                UNPROTECT(1);
                rowIndexes_[colIndex_] += 1;
                break;
            }

            Rcpp::List list;
            for (int i = 0; i < len; ++i) {
                list.push_back(ParseValue(node->elements[i],0));
            }

            bool childIsStruct = false;
            if(len > 0) {
                recordparser::NodePtr n = node->elements[0];
                if(n->type == orc::STRUCT) {
                    childIsStruct = true;
                }
            }

            if (childIsStruct) {
                //
                // If child is a struct convert it to a DF
                //
                uint64_t numRows = list.size();
                Rcpp::List firstRow = list[0];
                uint64_t numCols = firstRow.size();
                Rcpp::List df;
                for(uint64_t i = 0; i < numCols; ++i) {
                    Rcpp::List col;
                    for(uint64_t j = 0; j < numRows; ++j) {
                        Rcpp::List l = list[j];
                        col.push_back(l[i]);
                    }
                    DLOG(INFO) << *node.get();
                    df.push_back(col, (node->elements[0])->fieldNames[i]);

                }

                DLOG(INFO) << "STRUCT/LIST, Child is a struct. Transposing and making data frame " <<
                              " rows: " << numRows <<
                              " cols: " << numCols;

                Rcpp::StringVector rownames(len);
                for(uint64_t i = 0; i < len; ++i) {
                    rownames[i] = base::utils::to_string(i+1);
                }

                df.attr("class") = "data.frame";
                df.attr("row.names") = rownames;
                //df.attr("names") = ;

                SEXP val = PROTECT(Rcpp::wrap(df));
                SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
                UNPROTECT(1);
                rowIndexes_[colIndex_] += 1;

            }
            else  {
                SEXP val = PROTECT(Rcpp::wrap(list));
                SET_VECTOR_ELT(boost::get<SEXP>(columns_[colIndex_]), rowIndexes_[colIndex_], val);
                UNPROTECT(1);
                rowIndexes_[colIndex_] += 1;
            }
            break;
        }
        default: throw std::runtime_error("Unable to process unknown ORC record type");
    }
}


void RDataFrameAssembler::handleCsvRecord(AnyVector &v, boost::any& value) {
    recordparser::CsvRecord record = boost::any_cast<recordparser::CsvRecord>(value);

    switch (v.which()) {
        case 0:
        {
            // bool type
            BoolVectorPtr v2 = boost::get<BoolVectorPtr>(v);
            if (record.isNull) {
                nullBools_[colIndex_].push_back(v2->size());
                v2->push_back(NA_LOGICAL);
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding NULL record to col: " << colIndex_ << " type: bool";
            }
            else {
                bool v = boost::get<bool>(record.value);
                v2->push_back(v);
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding record " << v << " to col: " << colIndex_ << " type: bool";
            }
            ++recordsAssembled_;
            break;
        }
        case 1:
        {
            // int32 type
            Int32VectorPtr v2 = boost::get<Int32VectorPtr>(v);
            if (record.isNull) {
                v2->push_back(NA_INTEGER);
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding NULL record to col: " << colIndex_ << " type: int32_t";
            }
            else {
                int32_t v = boost::get<int32_t>(record.value);
                v2->push_back(v);
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding record " << v << " to col: " << colIndex_ << " type: int32_t";
            }
            ++recordsAssembled_;
            break;
        }
//        case 0:
//        {
//            // int64 type
//            IntegerVectorPtr v2 = boost::get<IntegerVectorPtr>(v);
//            if (record.isNull) {
//                v2->push_back(NA_INTEGER);
//                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding NULL record to col: " << colIndex_ << " type: int64_t";
//            }
//            else {
//                int64_t v = boost::get<int64_t>(record.value);
//                v2->push_back(v);
//                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding record " << v << " to col: " << colIndex_ << " type: int64_t";
//            }
//            ++recordsAssembled_;
//            break;
//        }
        case 2:
        {
            // double type
            DoubleVectorPtr v2 = boost::get<DoubleVectorPtr>(v);
            if (record.isNull) {
                v2->push_back(NA_REAL);
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding NULL record to col: " << colIndex_ << " type: double";
            }
            else {
                double v = boost::get<double>(record.value);
                v2->push_back(v);
                DLOG_IF(INFO,recordsAssembled_  < 10) << "adding record " << v << " to col: " << colIndex_ << " type: double";
            }
            ++recordsAssembled_;
            break;
        }
        case 3:
        {
            // string type
            CharacterVectorPtr v2 = boost::get<CharacterVectorPtr>(v);
            if (record.isNull) {
                nullStrings_[colIndex_].push_back(v2->size());
                v2->push_back("NA");
                DLOG_IF(INFO, recordsAssembled_ < 10) << "adding NULL record to col: " << colIndex_ << " type: string";
            }
            else {
                std::string v = boost::get<std::string>(record.value);
                v2->push_back(v);
                DLOG_IF(INFO,recordsAssembled_  < 10) << "adding record " << v << " to col: " << colIndex_ << " type: string";
            }
            ++recordsAssembled_;
            break;
        }


        default:
        {
            throw std::runtime_error("Unable to process unknown CSV record type");
        }
    }
}


boost::any RDataFrameAssembler::getObject()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }

    // register listener so we get notified when a section of the file is consumed
    recordParser_->registerListener(this);

    //
    // TODO maybe the orc and !orc cases can be merged
    //

    // if the file is ORC
    if(extension_ == "orc") {
        while(recordParser_->hasNext()) {
            boost::any value = recordParser_->next();
            handleOrcRecord(value);
        }

        boost::shared_ptr<Rcpp::DataFrame> df(new Rcpp::DataFrame());
        for(int i = 0; i < columns_.size(); ++i) {
            switch(columnTypes_[i]) {
//            case ORC_INT64_COL:
//                df->push_back(PROTECT(Rcpp::wrap(*((boost::get<IntegerVectorPtr>(columns_[i])).get()))), columnNames_[i]);
//                break;
            case ORC_BOOL_COL: {
                Rcpp::LogicalVector myv2 = PROTECT(Rcpp::wrap(*((boost::get<BoolVectorPtr>(columns_[i])).get()))); // copy vector to RcppVector

                // replace sliced NA_LOGICAL by proper NA
                std::vector<uint64_t> nullBools = nullBools_[i];
                for (int j = 0; j < nullBools.size(); ++j) {
                    myv2[nullBools[j]] = NA_LOGICAL;
                }

                df->push_back(myv2, columnNames_[i]); // add to DF
                break;
            }
            case ORC_INT32_COL:
                df->push_back(PROTECT(Rcpp::wrap(*((boost::get<Int32VectorPtr>(columns_[i])).get()))), columnNames_[i]);
                break;
            case ORC_DOUBLE_COL:
                df->push_back(PROTECT(Rcpp::wrap(*((boost::get<DoubleVectorPtr>(columns_[i])).get()))), columnNames_[i]);
                break;
            case ORC_STRING_COL: {
                Rcpp::CharacterVector myv2 = PROTECT(Rcpp::wrap(*((boost::get<CharacterVectorPtr>(columns_[i])).get()))); // copy vector to RcppVector

                // replace "NA" by proper NA
                std::vector<uint64_t> nullStrings = nullStrings_[i];
                for (int j = 0; j < nullStrings.size(); ++j) {
                    myv2[nullStrings[j]] = NA_STRING;
                }

                df->push_back(myv2, columnNames_[i]); // add to DF
                break;
            }
            case ORC_LIST_COL: {
                if (orcSchema_[i].second == orc::STRUCT) {
                    Rcpp::List col = boost::get<SEXP>(columns_[i]);
                    col.attr("class") = "AsIs";

                    DLOG(INFO) << "num children: " << col.size();

                    uint64_t numRows = col.size();
                    Rcpp::List firstRow = col[0];
                    uint64_t numCols = firstRow.size();
                    Rcpp::List df2;
                    for(uint64_t k = 0; k < numCols; ++k) {
                        Rcpp::List c;
                        for(uint64_t j = 0; j < numRows; ++j) {
                            if (TYPEOF(col[j]) == FREESXP) {
                                throw std::runtime_error("released by GC early");
                            }
                            Rcpp::List l = col[j];
                            c.push_back(l[k]);
                        }
                        if (k < topStructColumnNames_[i].size()) {
                            df2.push_back(c, topStructColumnNames_[i][k]);
                        }
                        else {
                            df2.push_back(c, std::string("Unknown"));
                        }
                    }

                    DLOG(INFO) << "STRUCT/LIST, Child is a struct. Transposing and making data frame " <<
                                  " rows: " << numRows <<
                                  " cols: " << numCols;

                    Rcpp::StringVector rownames(numRows);
                    for(uint64_t k = 0; k < numRows; ++k) {
                        rownames[k] = base::utils::to_string(k+1);
                    }

                    df2.attr("class") = "data.frame";
                    df2.attr("row.names") = rownames;

                    df->push_back(df2, columnNames_[i]);

                }
                else {
                    Rcpp::List col = boost::get<SEXP>(columns_[i]);
                    col.attr("class") = "AsIs";

                    df->push_back(col, columnNames_[i]);
                }
                break;
            }            
            default:
                throw std::runtime_error("unknown column type");
                break;
            }
        }
        UNPROTECT(columns_.size());  // this also accounts for the creation of list-type columns in configureOrc()
        Rcpp::StringVector rownames(numRows_);
        for(uint64_t i = 0; i < numRows_; ++i) {
            rownames[i] = base::utils::to_string(i+1);
        }
        df->attr("row.names") = rownames;
        df->attr("class") = "data.frame";
        return boost::any(df);
    }
    else {
        typedef std::map<int32_t, std::pair<std::string, std::string> >::iterator schema_it;
        typedef std::map<int32_t, std::pair<std::string, AnyVector> >::iterator vector_it;
        std::map<int32_t, std::pair<std::string, AnyVector> > vectors;

        // create 1 vector for each column
        int index = 0;
        for(schema_it it = schema_.begin(); it != schema_.end(); ++it) {
            //name,type
            std::pair<std::string, std::string> column = schema_[index];
            std::string columnName = column.first;
            std::string columnType = column.second;
            if(columnType == "logical") {
                DLOG(INFO) << "initializing column of type logical " << columnName << " index: " << index;
                vectors.insert(make_pair(index, make_pair(columnName, AnyVector(boost::shared_ptr<std::vector<bool> >(new std::vector<bool>())))));
            }
            else if(columnType == "integer") {
                DLOG(INFO) << "initializing column of type integer " << columnName << " index: " << index;
                vectors.insert(make_pair(index, make_pair(columnName, AnyVector(boost::shared_ptr<std::vector<int32_t> >(new std::vector<int32_t>())))));
            }
            else if(columnType == "int64") {
                DLOG(INFO) << "initializing column of type int64 " << columnName << " index: " << index;
                vectors.insert(make_pair(index, make_pair(columnName, AnyVector(boost::shared_ptr<std::vector<double> >(new std::vector<double>())))));
            }
            else if(columnType == "numeric") {
                DLOG(INFO) << "initializing column of type numeric " << columnName << " index: " << index;
                vectors.insert(make_pair(index, make_pair(columnName, AnyVector(boost::shared_ptr<std::vector<double> >(new std::vector<double>())))));
            }
            else if(columnType == "character") {
                DLOG(INFO) << "initializing column of type character " << columnName << " index: " << index;
                vectors.insert(make_pair(index, make_pair(columnName, AnyVector(boost::shared_ptr<std::vector<std::string> >(new std::vector<std::string>())))));
            }            
            else {
                std::ostringstream os;
                os << "Unsupported type ";
                os << columnType;
                os << ". Supported types are bool, integer, int64, numeric and character.";
                throw std::runtime_error(os.str());
            }
            // initialize nullBools_ and nullStrings_
            nullBools_[index] = std::vector<uint64_t>();
            nullStrings_[index] = std::vector<uint64_t>();
            ++index;
        }
        // process records
        while(recordParser_->hasNext()) {
            boost::any value;
            try {
                value = recordParser_->next();
            }
            catch(const splitproducer::EmptySplitException& e) {
                LOG(ERROR) << e.what();
                break;  // split is empty. Do not abort, we just generate an empty DF
            }
            catch(const std::runtime_error& e) {
                //LOG(ERROR) << e.what();
                throw;
            }

            if(colIndex_ >= (uint64_t)index) {
                throw std::runtime_error("out of bounds");
            }

            AnyVector v = vectors[colIndex_].second;
            handleCsvRecord(v, value);

            if(format_ == "row") {
                colIndex_ += 1;
            }

        }
        boost::shared_ptr<Rcpp::DataFrame> df(new Rcpp::DataFrame);
        uint64_t col = 0;
        for(vector_it it = vectors.begin(); it != vectors.end(); ++it) {
            std::pair<std::string, AnyVector> value = it->second;
            std::string columnName = value.first;
            AnyVector v = value.second;
            switch (v.which()) {
                case 0:
                {
                    // bool
                    DLOG(INFO) << "copying column of type bool " << columnName;
                    BoolVectorPtr v2 = boost::get<BoolVectorPtr>(v);
                    std::vector<bool> myv = *(v2.get());
                    Rcpp::LogicalVector myv2 = PROTECT(Rcpp::wrap(myv)); // copy vector to RcppVector

                    // replace "NA" by proper NA
                    std::vector<uint64_t> nullBools = nullBools_[col];
                    for (int i = 0; i < nullBools.size(); ++i) {
                        myv2[nullBools[i]] = NA_LOGICAL;
                    }

                    df->push_back(myv2, columnName); // add to DF
                    break;
                }
                case 1:
                {
                    // int32
                    DLOG(INFO) << "copying column of type int32 " << columnName;
                    Int32VectorPtr v2 = boost::get<Int32VectorPtr>(v);
                    std::vector<int32_t> myv = *(v2.get());
                    // R integers are only 32 bits so we use the numeric type
                    Rcpp::IntegerVector myv2 = PROTECT(Rcpp::wrap(myv)); // copy vector to RcppVector
                    df->push_back(myv2, columnName); // add to DF
                    break;
                }
                case 2:
                {
                    // double
                    DLOG(INFO) << "copying column of type double " << columnName;
                    DoubleVectorPtr v2 = boost::get<DoubleVectorPtr>(v);
                    std::vector<double> myv = *(v2.get());
                    Rcpp::DoubleVector myv2 = PROTECT(Rcpp::wrap(myv)); // copy vector to RcppVector
                    df->push_back(myv2, columnName); // add to DF
                    break;
                }
                case 3:
                {
                    // string
                    DLOG(INFO) << "copying column of type string " << columnName;
                    CharacterVectorPtr v2 = boost::get<CharacterVectorPtr>(v);
                    std::vector<std::string> myv = *(v2.get());
                    Rcpp::CharacterVector myv2 = PROTECT(Rcpp::wrap(myv)); // copy vector to RcppVector

                    // replace "NA" by proper NA
                    std::vector<uint64_t> nullStrings = nullStrings_[col];
                    for (int i = 0; i < nullStrings.size(); ++i) {
                        myv2[nullStrings[i]] = NA_STRING;
                    }

                    df->push_back(myv2, columnName); // add to DF
                    break;
                }

                default:
                {
                    throw std::runtime_error("Unsupported  vector type");
                }
            }
            col += 1;
        }
        UNPROTECT(vectors.size());
        return boost::any(df);
    }
}

void RDataFrameAssembler::update(int32_t level)
{
    if(level == 0) {
        if(extension_ == "csv") {
            if (colIndex_ != schema_.size()) {
                std::ostringstream os;
                os << "CSV Row (" << (totalRowsProcessed_ + 1) <<
                      ") has less records (" << colIndex_ <<
                      ") than the number "
                      "specified in the schema (" << schema_.size() << ")." <<
                      " Also make sure the delimiter is correct.";
                throw std::runtime_error(os.str());
            }
        }

        // check that all the rows have the same number of records
        if (numColsInLastRow_ == -1) {
            // don't check the 1st row as we have nothing to compare it with
        }
        else {
            if(extension_ == "csv") {
                if(colIndex_ != numColsInLastRow_) {
                    std::ostringstream os;
                    os << "Malformed CSV file. "
                          "Number of columns in row " << (totalRowsProcessed_ + 1) <<
                          " (" << colIndex_ << ") is different from previous row (" <<
                          numColsInLastRow_ << ").";
                    throw std::runtime_error(os.str());
                }
            }
        }

        numColsInLastRow_ = colIndex_;  // update

        DLOG_IF(INFO, splitsAssembled_ < 10) << "split completed, resetting index!";
        ++splitsAssembled_;
        if(format_ == "row") {
            colIndex_ = 0;
        }
        else if(format_ == "column") {
            ++colIndex_;
        }
        ++totalRowsProcessed_;
    }
    else if (level == 1) {
        if(extension_ == "orc") {
            DLOG(INFO) << "Initializing new stripe";
            colIndex_ = 0;
        }
        else {
            throw std::runtime_error("unsupported level");
        }
    }
    else {
        throw std::runtime_error("unsupported level");
    }
}

}  // namespace assembler
}  // namespace ddc

