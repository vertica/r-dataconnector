#include "orcrecordparser.h"
#include <stdexcept>
#include <boost/algorithm/minmax_element.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <glog/logging.h>
#include "base/utils.h"
#include "hdfsutils/hdfsinputstream.h"

namespace orc {
    std::string toDecimalString(int64_t value, int32_t scale);
}

namespace ddc {
namespace recordparser {

OrcRecordParser::OrcRecordParser()
    : configured_(false),
      currentColumn_(NULL),
      numStripes_(0),
      numColumns_(0),
      numTopColumns_(0),
      numRows_(0),
      numRowsInThisStripe_(0),
      rowIndex_(0),
      colIndex_(0),
      topColIndex_(0),
      stripeIndex_(0),
      colsInLastComplexType_(0),
      colNameIndex_(0)
{
    observer_ = NULL;
    colIndexNameResetPoints_.push_back(0);
}

OrcRecordParser::~OrcRecordParser()
{
}

static void getKinds(const orc::Type& type,
              std::vector<orc::TypeKind>& kinds,
              std::vector<uint64_t>& topIndexes,
              int level);

static void getKinds(const orc::Type& type,
              std::vector<orc::TypeKind>& kinds,
              std::vector<uint64_t>& topIndexes)
{
    getKinds(type, kinds, topIndexes, 0);
}

void getKinds(const orc::Type& type,
              std::vector<orc::TypeKind>& kinds,
              std::vector<uint64_t>& topIndexes,
              int level)
{
    if(level == 1) { //level 0 is main STRUCT col
        topIndexes.push_back(kinds.size());
    }
    orc::TypeKind kind = type.getKind();
    kinds.push_back(kind);
    if(kind == orc::STRUCT ||
       kind == orc::MAP ||
       kind == orc::LIST) {
        for(uint64_t j = 0; j < type.getSubtypeCount(); ++j) {
            const orc::Type& subtype = type.getSubtype(j);
            getKinds(subtype, kinds, topIndexes, level + 1);
        }
    }

}




void OrcRecordParser::configure(base::ConfigurationMap &conf)
{
    GET_PARAMETER(url_, std::string, "url");
    std::string protocol = base::utils::getProtocol(url_);

    std::string filename;
    if(protocol != "") {
        filename = base::utils::removeSubstrs(url_, protocol + "://");
    }
    else {
        filename = url_;
    }

    orc::ReaderOptions opts;

    if(protocol == "hdfs" || protocol == "http" || protocol == "webhdfs") {
        hdfsutils::HdfsInputStream *p = new hdfsutils::HdfsInputStream(filename);
        base::ConfigurationMap hdfsconf;
        GET_PARAMETER(hdfsconf["hdfsConfigurationFile"], std::string, "hdfsConfigurationFile");
        p->configure(hdfsconf);
        std::unique_ptr<orc::InputStream> inputStream(p);
        orcReader_ = orc::createReader(std::move(inputStream), opts);
    }
    else {
        orcReader_ = orc::createReader(orc::readLocalFile(filename), opts);
    }
    numStripes_ = orcReader_->getNumberOfStripes();

    GET_PARAMETER(selectedStripes_, std::vector<uint64_t> , "selectedStripes");
    if (selectedStripes_.size() == 0) {
        // when no stripes are selected we read all of them
        for(uint64_t i = 0; i < numStripes_; ++i) {
            selectedStripes_.push_back(i);
        }
    }

    bool firstStripe = true;
    uint64_t startRow = 0;
    uint64_t endRow = 0;
    uint64_t rowIndex = 0;
    // choose stripes
    for(uint64_t i = 0; i < numStripes_; ++i) {
        uint64_t numCols = orcReader_->getStripeStatistics(i)->getNumberOfColumns();
        std::unique_ptr<orc::StripeInformation> stripeInfo = orcReader_->getStripe(i);
        uint64_t numRows = stripeInfo->getNumberOfRows();

        DLOG_IF(INFO, (i < 10)) << "stripe: " << i <<
                      " rows: " << numRows <<
                      " cols: " << numCols;
        //
        // TODO we only support contiguous stripes
        //
        if(std::find(selectedStripes_.begin(), selectedStripes_.end(), i) != selectedStripes_.end()) {
            stripeNumRows_.push_back(numRows);
            stripeRowOffsets_.push_back(endRow);
            // stripe selected
            if(firstStripe) {
                startRow = rowIndex;
                endRow += startRow;
                firstStripe = false;
            }
            endRow += numRows;
        }
        rowIndex += numRows;
    }

    DLOG(INFO) << "parsing from row: " << startRow << " to row: " << endRow;


//    if(numStripes_ == 0) {
//        throw std::runtime_error("no stripes");
//    }

//    if(endRow <= startRow) {
//        throw std::runtime_error(" endRow <= startRow");
//    }

    if(numStripes_ > 0) {
        numColumns_ = orcReader_->getStripeStatistics(0)->getNumberOfColumns();
    }
    numRows_ = endRow - startRow;

    stripeRowOffsets_.push_back(numRows_);

    DLOG(INFO) << "rows: " << numRows_
              << " cols: " << numColumns_;

    std::pair<std::vector<uint64_t>::iterator, std::vector<uint64_t>::iterator> p =
        boost::minmax_element(stripeNumRows_.begin(), stripeNumRows_.end());
//    uint64_t minNumRows = 0;
    uint64_t maxNumRows = 0;
//    if(p.first != stripeNumRows_.end()) {
//        minNumRows = *p.first;
//    }
    if(p.second != stripeNumRows_.end()) {
        maxNumRows = *p.second;
    }

    orcReader_->seekToRow(startRow);
    batch_ = orcReader_->createRowBatch(maxNumRows); //TODO create a batch whose size is the biggest stripe

    // initialize stats
    for(uint64_t i = 0; i < numColumns_; ++i) {
        nullCount_[i] = 0;
    }

    const orc::Type& type = orcReader_->getType();

    getKinds(type, columnKinds_, topIndexes_);
    getColNames(type);

    configured_ = true;
}



//TODO this file needs heavy refactoring

// copied from the ORC library with minor changes
std::string timeStampToString(int64_t timestamp) {
    const int64_t NANOS_PER_SECOND = 1000000000;
    const int64_t NANO_DIGITS = 9;

    time_t epoch;
    struct tm epochTm;
    epochTm.tm_sec = 0;
    epochTm.tm_min = 0;
    epochTm.tm_hour = 0;
    epochTm.tm_mday = 1;
    epochTm.tm_mon = 0;
    epochTm.tm_year = 70;
    epochTm.tm_isdst = 0;
    epoch = mktime(&epochTm);

    std::string str;
    int64_t nanos = timestamp % NANOS_PER_SECOND;
    time_t seconds =
      static_cast<time_t>(timestamp / NANOS_PER_SECOND) + epoch;
    // make sure the nanos are positive
    if (nanos < 0) {
      seconds -= 1;
      nanos = -nanos;
    }
    struct tm tmValue;
    localtime_r(&seconds, &tmValue);
    char timeBuffer[20];
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);

    str += "\"";
    str += timeBuffer;
    str += ".";

    // remove trailing zeros off the back of the nanos value.
    int64_t zeroDigits = 0;
    if (nanos == 0) {
      zeroDigits = 8;
    } else {
      while (nanos % 10 == 0) {
        nanos /= 10;
        zeroDigits += 1;
      }
    }
    char numBuffer[64];
    snprintf(numBuffer, sizeof(numBuffer), "%0*ld\"",
             static_cast<int>(NANO_DIGITS - zeroDigits),
             static_cast<int64_t >(nanos));
    str += std::string(numBuffer, strlen(numBuffer));
    return str;
}

void OrcRecordParser::getRecord(orc::ColumnVectorBatch* column,
                                orc::TypeKind kind,
                                uint64_t rowId,
                                uint64_t nestedLevel,
                                NodePtr  record,
                                NodePtr parent)
{
    if (nestedLevel == 0) {
        record->isNull = false;
        if(column->hasNulls && !column->notNull[rowId]) {
            nullCount_[colIndex_] += 1;
            record->isNull = true;
        }
    }

    if(rowId >= column->numElements) {
        throw std::runtime_error("out of bounds");
    }

    switch(kind) {
        case orc::BOOLEAN:
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG: {

            int64_t* buffer = ((orc::LongVectorBatch *)column)->data.data();

            if(nestedLevel == 0) {
                record->type = kind;
                record->value = record->isNull ? 0 : buffer[rowId];
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind;
                node1->value = (column->hasNulls && !column->notNull[rowId]) ? 0 :
                                   buffer[rowId]; node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }

//            int64_t value = record->isNull ? 0 : buffer[rowId];
//            DLOG_IF(INFO, (rowId < 4) ) << "LONG col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << value <<
//                                            " level: " << nestedLevel;

            break;
        }
        case orc::FLOAT:
        case orc::DOUBLE: {
            double* buffer = ((orc::DoubleVectorBatch *)column)->data.data();
            if(nestedLevel == 0) {
                record->type = kind;
                record->value = record->isNull ? 0.0 : buffer[rowId];
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind;
                node1->value = (column->hasNulls && !column->notNull[rowId]) ? 0.0 :
                                   buffer[rowId]; node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{

                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }
//            double value = record->isNull ? 0.0 : buffer[rowId];
//            DLOG_IF(INFO, rowId < 4) << "DOUBLE col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << value;
            break;
        }
        case orc::BINARY:
        case orc::VARCHAR:
        case orc::CHAR:
        case orc::STRING: {
            char** buffer = ((orc::StringVectorBatch *)column)->data.data();
            int64_t* len = ((orc::StringVectorBatch *)column)->length.data();
            if(nestedLevel == 0) {
                record->type = kind;
                record->value = record->isNull ? std::string("NULL") : std::string(buffer[rowId],len[rowId]);
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind;
                node1->value = (column->hasNulls && !column->notNull[rowId]) ? std::string("NULL") :
                                   std::string(buffer[rowId],len[rowId]); node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }
//            std::string value = record->isNull ? std::string("NULL") : std::string(buffer[rowId],len[rowId]);
//            DLOG_IF(INFO, (rowId < 4) ) << "STRING col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << value <<
//                                            " level: " << nestedLevel;
            break;
        }

        case orc::DATE: {
            int64_t* buffer = ((orc::LongVectorBatch *)column)->data.data();
            char timeBuffer[11];
            if ((column->hasNulls && !column->notNull[rowId])) {
                timeBuffer[0] = '\0';
            }
            else {
                const time_t timeValue = buffer[rowId] * 24 * 60 * 60;
                struct tm tmValue;
                gmtime_r(&timeValue, &tmValue);
                strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d", &tmValue);
            }

            if(nestedLevel == 0) {
                record->type = kind;
                record->value = std::string(timeBuffer);
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind; node1->value = std::string(timeBuffer);
                node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }
//            DLOG_IF(INFO, (rowId < 4) && (nestedLevel == 0)) << "DATE col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << std::string(timeBuffer);
            break;
        }
        case orc::TIMESTAMP: {
            int64_t* buffer = ((orc::LongVectorBatch *)column)->data.data();
            std::string str;
            if ((column->hasNulls && !column->notNull[rowId])) {
                str = "";
            }
            else {
                str = timeStampToString(buffer[rowId]);
            }
            if(nestedLevel == 0) {
                record->type = kind;
                record->value = str;
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind; node1->value = timeStampToString(buffer[rowId]);
                node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }
//            DLOG_IF(INFO, (rowId < 4) && (nestedLevel == 0)) << "TIMESTAMP col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << str;
            break;
        }
        case orc::UNION: {
            orc::UnionVectorBatch *unionCol = (orc::UnionVectorBatch *)column;

            if(nestedLevel == 0) {
                record->type = kind;
                record->value = "unimplemented";
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind; node1->value = "unimplemented";
                node1->isNull = (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }

//            DLOG_IF(INFO, (rowId < 4) && (nestedLevel == 0)) << "UNION col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " tag: " << (unionCol->tags)[rowId] <<
//                                         " childOffset: " << (unionCol->offsets)[rowId];

            break;
        }
        case orc::DECIMAL: {
            int64_t* buffer = ((orc::Decimal64VectorBatch *)column)->values.data();
            if(nestedLevel == 0) {
                record->type = kind;
                record->value = record->isNull ? std::string("") : orc::toDecimalString(buffer[rowId], 5);
            }
            else {
                assert(parent);
                NodePtr node1(new Node); node1->type = kind;
                node1->value = (column->hasNulls && !column->notNull[rowId]) ? 0 :
                                   orc::toDecimalString(buffer[rowId], 5); (column->hasNulls && !column->notNull[rowId]);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node1);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node1);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }
//            std::string value = record->isNull ? std::string("") : orc::toDecimalString(buffer[rowId], 5);
//            DLOG_IF(INFO, rowId < 4) << "DECIMAL col: " << colIndex_ <<
//                                         " rowId: " << rowId <<
//                                         " value: " << value;//TODO hardcoded scale
            break;
        }
        case orc::LIST: {


            int64_t* buffer = ((orc::ListVectorBatch *)column)->offsets.data();

            uint64_t len;
            if ((column->hasNulls && !column->notNull[rowId])) {
                len = 0;
            }
            else {
                len = buffer[rowId + 1] - buffer[rowId];
            }
//            DLOG_IF(INFO, (rowId < 4) ) << "LIST col: " << colIndex_ <<
//                                     " rowId: " << rowId <<
//                                     " list len: " << len <<
//                                            " level: " << nestedLevel;


            uint64_t listIndex = colIndex_;

            orc::ColumnVectorBatch* col = ((orc::ListVectorBatch *)column)->elements.get();

            NodePtr node;

            if(nestedLevel == 0) {
                record->type = kind;
                node = record;
            }
            else {
                node = NodePtr(new Node);
                node->type = kind;
                node->isNull = (column->hasNulls && !column->notNull[rowId]);
                assert(parent);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }

            //
            // TODO even if the list has 0 elements we need to advance the subcolumns
            //
            if(len == 0) {
                colIndex_ = topIndexes_[topColIndex_ + 1] - 1;
                //DLOG(INFO) << "advancing col index to " << colIndex_;
            }

            // LIST always has a child STRUCT column
            for(uint64_t i = 0; i < len; ++i) {
                colIndex_ = listIndex + 1;
                // recurse
                getRecord(col,
                          columnKinds_[colIndex_],
                          buffer[rowId] + i,
                          nestedLevel + 1,
                          record,
                          node);
            }

            break;
        }
        case orc::MAP: {
//            DLOG_IF(INFO, (rowId < 4) ) << "MAP col: " << colIndex_ <<
//                                     " rowId: " << rowId <<
//                                            " level: " << nestedLevel;
            int64_t* buffer = ((orc::MapVectorBatch *)column)->offsets.data();
            uint64_t len;
            if ((column->hasNulls && !column->notNull[rowId])) {
                len = 0;
            }
            else {
                len = buffer[rowId + 1] - buffer[rowId];
            }

            //
            // TODO is this a bug in the ORC library?
            // Each stripe is 5000 rows. Total number of rows is 32k
            // When we request to read all the 32k rows, lenghts are 0 after
            // the first 5000 rows so we end up with 0 - index = really big number
            //

            orc::ColumnVectorBatch* keyCol = ((orc::MapVectorBatch *)column)->keys.get();
            orc::ColumnVectorBatch* valueCol = ((orc::MapVectorBatch *)column)->elements.get();
            uint64_t mapIndex = colIndex_;

            NodePtr node;

            if(nestedLevel == 0) {
                record->type = kind;
                node = record;
            }
            else {
                node = NodePtr(new Node);
                node->type = kind;
                node->isNull = (column->hasNulls && !column->notNull[rowId]);
                assert(parent);
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }
                }
            }

            if(len == 0) {
                colIndex_ = topIndexes_[topColIndex_ + 1] - 1;
                //DLOG(INFO) << "advancing col index to " << colIndex_;
            }


            for(uint64_t i = 0; i < len; ++i) {
                //recurse for K/V
                colIndex_ = mapIndex + 1;
                getRecord(keyCol,
                          columnKinds_[colIndex_],
                          buffer[rowId] + i,
                          nestedLevel + 1,
                          record,
                          node);

                colIndex_ = mapIndex + 2;
                getRecord(valueCol,
                          columnKinds_[colIndex_],
                          buffer[rowId] + i,
                          nestedLevel + 1,
                          record,
                          node);

                //
                // terrible hack
                // the 2 previous getRecord() calls have added
                // 2 entries to node->kvpairs
                // Remove those 2 entries and append a single pair {k,v} entry
                // TODO not sure it this works for nested maps
                //

                std::pair<NodePtr, NodePtr> testvalue = node->kvpairs[node->kvpairs.size() - 1];
                node->kvpairs.pop_back();

                std::pair<NodePtr, NodePtr> testkey = node->kvpairs[node->kvpairs.size() - 1];
                node->kvpairs.pop_back();

                std::pair<NodePtr,NodePtr> pair = std::make_pair(testkey.second, testvalue.second);
                node->kvpairs.push_back(pair);
//                DLOG_IF(INFO, (rowId < 4)) << "key: " << *(testkey.second.get()) <<
//                              " value: " << *(testvalue.second.get());
            }

            break;

        }
        case orc::STRUCT: {
            //
            // TODO get column names and store in the node structure
            //

//            DLOG_IF(INFO, (rowId < 4)) << "STRUCT col: " << colIndex_ <<
//                                     " rowId: " << rowId <<
//                                           " level: " << nestedLevel;

            orc::StructVectorBatch *tmp = (orc::StructVectorBatch*)column;
            uint64_t numChildren;
            if ((column->hasNulls && !column->notNull[rowId])) {
                numChildren = 0;
            }
            else {
                numChildren = (tmp)->fields.size();
            }

            NodePtr node;

            uint64_t colNameIndexSave = colNameIndex_;
            if(nestedLevel == 0) {
                record->type = kind;
                node = record;
                for(uint64_t i = 0; i < numChildren; ++i) {
                    node->fieldNames.push_back(getColName(colNameIndex_));
                    ++colNameIndex_;
                }
            }
            else {
                assert(parent);
                node = NodePtr(new Node);
                node->type = kind;
                node->isNull = (column->hasNulls && !column->notNull[rowId]);
                for(uint64_t i = 0; i < numChildren; ++i) {
                    node->fieldNames.push_back(getColName(colNameIndex_));
                    ++colNameIndex_;
                }
                switch(parent->type) {
                    case orc::MAP: {
                        NodePtr key(new Node); key->type = orc::STRING; key->value = "key";
                        std::pair<NodePtr,NodePtr> pair = std::make_pair(key, node);
                        parent->kvpairs.push_back(pair);
                        break;
                    }
                    case orc::STRUCT:
                    case orc::LIST:{
                        parent->elements.push_back(node);
                        break;
                    }
                    default: {
                        throw std::runtime_error("In switch default");
                    }

                }
            }


            for(uint64_t i = 0; i < numChildren; ++i) {
                ++colIndex_;
                getRecord(((orc::StructVectorBatch *)column)->fields.at(i),
                          columnKinds_[colIndex_],
                          rowId,
                          nestedLevel + 1,
                          record,
                          node);
            }


            colNameIndex_ = colNameIndexSave;

//            DLOG_IF(INFO, (rowId < 4)) << "STRUCT node: " << *(node.get());
            break;
        }

        default: {
            break;
        }
    }
    return;
}

void traverse(NodePtr node) {
    traverse(node, 0);
}
void traverse(NodePtr node, int level) {
    switch(node->type) {
        case orc::BOOLEAN:
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
        case orc::FLOAT:
        case orc::DOUBLE:
        case orc::STRING:
        case orc::BINARY:
        case orc::TIMESTAMP:
        case orc::UNION:
        case orc::DECIMAL:
        case orc::DATE:
        case orc::VARCHAR:
        case orc::CHAR:

        {
            DLOG(INFO) << node->value << ", ";
            break;
        }
        case orc::STRUCT: {
            DLOG(INFO) << "{";
            for(uint64_t i = 0; i < node->elements.size(); ++i) {
                traverse(node->elements[i], level + 1);
            }
            DLOG(INFO) << "}, ";
            break;
        }

        case orc::LIST: {
            DLOG(INFO) << "[";
            for(uint64_t i = 0; i < node->elements.size(); ++i) {
                traverse(node->elements[i], level + 1);
            }
            DLOG(INFO) << "], ";
            break;
        }
        case orc::MAP: {
            DLOG(INFO) << "{";
            for(uint64_t i = 0; i < node->kvpairs.size(); ++i) {
                DLOG(INFO) << "\"";
                //key
                traverse((node->kvpairs[i]).first, level + 1);
                DLOG(INFO) << "\"";

                DLOG(INFO) << ": \"";
                //value
                traverse((node->kvpairs[i]).second, level + 1);
                DLOG(INFO) << "\", ";
            }
            DLOG(INFO) << "}, ";
            break;
        }
        default: throw std::runtime_error("unimplemented");
    }
}

void OrcRecordParser::initializeStripe() {


    orc::TypeKind kind = orcReader_->getType().getKind();

    if(kind == orc::STRUCT) {
        numTopColumns_ = ((orc::StructVectorBatch*)(batch_.get()))->fields.size();

        if(numTopColumns_ < 1) {
            LOG(WARNING) << "less than 1 children";
            currentColumn_ = NULL;
        }
        else {
            currentColumn_ = ((orc::StructVectorBatch*)(batch_.get()))->fields[0];
        }
        colIndex_ = 1; //skip col0 (struct)
    }
    else {
        numTopColumns_ = numColumns_;
        currentColumn_ = batch_.get();
        colIndex_ = 0;
    }

    topColIndex_ = 0;

    bool res = orcReader_->next(*batch_); //TODO check res
    if(!res) {
        throw std::runtime_error("0 rows read");
    }

    assert((stripeIndex_ + 1) < stripeRowOffsets_.size());
    numRowsInThisStripe_ = stripeRowOffsets_[stripeIndex_ + 1] - stripeRowOffsets_[stripeIndex_];
    DLOG(INFO) << "Initializing a new stripe of " << numRowsInThisStripe_ << " rows";
    ++stripeIndex_;

    rowIndex_ = 0;
}

void OrcRecordParser::getColNames(const orc::Type &type) {
    getColNames(type, 0);
}

void OrcRecordParser::getColNames(const orc::Type &type, int level) {
    if(type.getKind() == orc::STRUCT ||
            type.getKind() == orc::MAP ||
            type.getKind() == orc::LIST) {
        for(int i = 0; i < type.getSubtypeCount(); ++i) {
            if(type.getKind() == orc::STRUCT) {
                if(level > 0) {
                    DLOG(INFO) << "level: " << level <<
                                  " key: " << type.getFieldName(i);
                    colnames_.push_back(type.getFieldName(i));
                }
            }
            getColNames(type.getSubtype(i),level + 1);
        }
    }
    if(level == 1) {
        colIndexNameResetPoints_.push_back(colnames_.size());
    }
    return;
}

boost::any OrcRecordParser::next()
{
    if(!configured_) {
        throw std::runtime_error("not configured");
    }

    if((rowIndex_ == 0) ||
       ((rowIndex_ == numRowsInThisStripe_) && (topColIndex_ == (numTopColumns_ - 1)))) {

        if(rowIndex_ == numRowsInThisStripe_) {
            if(!observer_) {
                throw std::runtime_error("assembler not configured");
            }
            else {
                observer_->update(1);  // we use "1" for new stripe, "0" for new column
            }
        }

        initializeStripe();
    }
    else if(rowIndex_ == numRowsInThisStripe_) {
        rowIndex_ = 0;
        ++topColIndex_;
        colNameIndex_ = colIndexNameResetPoints_[topColIndex_];
        colIndex_ += colsInLastComplexType_;
        DLOG(INFO) << "Advancing colIndex " << colsInLastComplexType_ << " positions. " <<
                      " current col index: " << colIndex_ << " topcolindex: " << topColIndex_;
        currentColumn_ =
                ((orc::StructVectorBatch*)(batch_.get()))->fields[topColIndex_];
        if(!observer_) {
            throw std::runtime_error("assembler not configured");
        }
        else {
            observer_->update(0);
        }
    }
    if(colIndex_ >= numColumns_) {
        throw std::runtime_error("no more records");
    }
    uint64_t currentCol = colIndex_;
    NodePtr nullNode;
    NodePtr record(new Node);
    getRecord(currentColumn_,
              columnKinds_[colIndex_],
              rowIndex_,
              0,  // nestedLevel
              record,
              nullNode);
    //traverse(record);
    DLOG_IF(INFO, (rowIndex_ < 4) )  << "record: " <<  *(record.get());
    boost::any res = record;
#if 0
    if(record->type == orc::LIST ||
            record->type == orc::STRUCT ||
            record->type == orc::MAP) {
        res = record;
    }
    else {
        std::string& str = boost::get<std::string>(record->value);
        res = boost::any(str); //TODO hardcoded for PRIMITIVE=STRING
    }
#endif
    colsInLastComplexType_ = colIndex_ - currentCol + 1;
    colIndex_ = currentCol;  //need to reset colIndex_ for complex types
    ++rowIndex_;
    return res;

}

bool OrcRecordParser::hasNext()
{
    bool inLastStripe = (stripeIndex_ == (selectedStripes_.size()));
    bool inLastColumn = ((topColIndex_ == (numTopColumns_ - 1)) || (numTopColumns_ == 0));
    return  !inLastStripe ||
            !inLastColumn ||
            (inLastStripe && inLastColumn && (rowIndex_ < numRowsInThisStripe_));

}

static std::string printNode(const Node& node) {
    std::ostringstream stream;

    //stream << orc::kind2String(node.type);
    switch(node.type) {
    case orc::BOOLEAN:
    case orc::BYTE:
    case orc::SHORT:
    case orc::INT:
    case orc::LONG: {
        if (node.isNull)
            stream << "NULL";
        else
            stream << boost::get<int64_t>(node.value);
        return stream.str();
    }
    case orc::FLOAT:
    case orc::DOUBLE: {
        if (node.isNull)
            stream << "NULL";
        else
            stream << boost::get<double>(node.value);
        return stream.str();
    }
    case orc::BINARY:
    case orc::STRING:
    case orc::DECIMAL:
    case orc::TIMESTAMP:
    case orc::DATE:
    case orc::VARCHAR:
    case orc::CHAR:
        if (node.isNull)
            stream << "NULL";
        else
            stream << "\"" << boost::get<std::string>(node.value) << "\"";
        return stream.str();
    case orc::MAP:
        if (node.isNull) {
            stream << "NULL";
        }
        else {
            //stream << "kvpairs: " << node.kvpairs.size() << " -> {";
            stream << "{";
            for(uint64_t i = 0; i < node.kvpairs.size(); ++i) {
                stream << printNode(*(node.kvpairs[i].first.get())) <<
                          ": " << printNode(*(node.kvpairs[i].second.get())) << ", ";
            }
            stream << "}";
        }
        return stream.str();
    case orc::STRUCT:
    case orc::LIST:
        if (node.isNull) {
            stream << "NULL";
        }
        else {
//            stream << " elements: " << node.elements.size() << " -> [";
            stream << "[";
//            if(node.type == orc::STRUCT) {
//                for(uint64_t i = 0; i < node.fieldNames.size(); ++i) {
//                    stream << node.fieldNames[i] << ",";
//                }
//                stream << std::endl;
//            }
            for(uint64_t i = 0; i < node.elements.size(); ++i) {
                stream << printNode(*(node.elements[i].get())) << ", ";
            }
            stream << "]";
        }
        return stream.str();
    default:  {
        throw std::runtime_error("in default case");
    }
    }

}

std::ostream &operator<<(std::ostream &stream, const Node &node) {
    stream << printNode(node);
    return stream;
}


}  // namespace recordparser
}  // namespace ddc
