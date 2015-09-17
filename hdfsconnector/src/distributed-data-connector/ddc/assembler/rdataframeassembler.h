#ifndef DDC_ASSEMBLER_RDATAFRAMEASSEMBLER_H
#define DDC_ASSEMBLER_RDATAFRAMEASSEMBLER_H

#include <map>
#include <stdexcept>
#include <string>
#include <utility>

#include <boost/algorithm/string/replace.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>

#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>

#include "assembler/iassembler.h"
#include "base/utils.h"
#include "hdfsutils/hdfsinputstream.h"
#include "hdfsutils/hdfsutils.h"
#include "recordparser/csvrecordparser.h"
#include "recordparser/orcrecordparser.h"
#include "recordparser/recordparserfactory.h"


//include R the last, otherwise overwrites a lot of stuff
//#include <R.h>
//#include <Rinternals.h>
#include <Rcpp.h>

namespace ddc {
namespace assembler {

typedef boost::variant<BoolVectorPtr,
                       Int32VectorPtr,
                       DoubleVectorPtr,
                       CharacterVectorPtr,
                       SEXP> AnyVector;

typedef boost::shared_ptr<Rcpp::NumericVector> RcppNumericVectorPtr;
typedef boost::shared_ptr<Rcpp::CharacterVector> RcppCharacterVectorPtr;
typedef boost::shared_ptr<Rcpp::List> RcppListPtr;
typedef boost::shared_ptr<Rcpp::LogicalVector> RcppLogicalVectorPtr;
typedef boost::shared_ptr<Rcpp::IntegerVector> RcppIntegerVectorPtr;


namespace testing {
    class FakeAssemblerTest;
}  // namespace testing

/**
 * @brief Assembles R dataframe objects
 */
class RDataFrameAssembler: public IAssembler
{

    friend class testing::FakeAssemblerTest;
public:
    RDataFrameAssembler();
    ~RDataFrameAssembler();

    void configure(base::ConfigurationMap& conf);

    boost::any getObject();

    void update(int32_t level);

private:
    /**
     * @brief configureOrc
     */
    void configureOrc();
    /**
     * @brief handleOrcRecord Called for every ORC record read.
     * @param record
     */
    void handleOrcRecord(boost::any& record);
    /**
     * @brief handleCsvRecord Called for every CSV record read.
     * @param v
     * @param value
     */
    void handleCsvRecord(AnyVector &v, boost::any& value);

    /**
     * @brief ParseValue
     * @param node
     * @param level Recursion level
     * @return
     */
    SEXP ParseValue(recordparser::NodePtr& node, int level);

    /**
     */
    uint64_t colIndex_;

    /**
     */
    uint64_t numRows_;

    /**
     */
    uint64_t numCols_;

    /**
     */
    uint64_t numTopCols_;

    /**
     */
    uint64_t totalRowsProcessed_;

    /**
     * Can be row (CSV) or columnar (ORC)
     */
    std::string format_;

    /**
     */
    recordparser::IRecordParserPtr recordParser_;

    /**
     * Column number -> (column name, column type)
     * e.g. { 0 -> age, int64
     *        1 -> name, string }
     * Only used for CSV files.
     * Valid types are int64, double and string
     */
    std::map<int32_t, std::pair<std::string, std::string> > schema_;

    /**
     * Column number -> (column name, column type)
     * e.g. { 0 -> age, INT
     *        1 -> name, STRING }
     * Only used for ORC files.
     */
    std::map<int32_t, std::pair<std::string, orc::TypeKind> > orcSchema_; // for ORC

    /**
     * Keep some statistics
     */
    uint64_t recordsAssembled_;
    uint64_t splitsAssembled_;

    /**
     * Whether the object has been configured
     */
    bool configured_;

    /**
     * Full URL of the file to download
     */
    std::string url_;

    /**
     * Extension of the file
     */
    std::string extension_;

    /**
     * Filename of the HDFS configuration file
     */
    std::string hdfsConfigurationFile_;

    /**
     * Actual columns containing the data
     */
    std::vector<AnyVector> columns_;

    /**
     * Column names
     */
    std::vector<std::string> columnNames_;

    /**
     * Column types
     */
    enum ColumnType {
        ORC_BOOL_COL,
        ORC_INT32_COL,
        ORC_INT64_COL,
        ORC_DOUBLE_COL,
        ORC_STRING_COL,
        ORC_LIST_COL
    };
    std::vector<ColumnType> columnTypes_;

    /**
     * Column names only for STRUCT columns
     */
    std::map<int, std::vector<std::string> > topStructColumnNames_;

    /**
     * How many column in the last row consumed
     */
    int64_t numColsInLastRow_;

    /**
     * Keeps the row index for each column.
     * Useful for SET_VECTOR_ELT(SEXP, index, value)
     */
    typedef std::vector<uint64_t> RowIndexes;
    RowIndexes rowIndexes_;

    /**
     * Keeps track of null bools
     * colIndex, rowIndex
     */
    std::map<uint64_t, std::vector<uint64_t> > nullBools_;

    /**
     * Keeps track of null strings
     * colIndex, rowIndex
     */
    std::map<uint64_t, std::vector<uint64_t> > nullStrings_;

};

typedef boost::shared_ptr<RDataFrameAssembler> RDataFrameAssemblerPtr;

}  // namespace assembler
}  // namespace ddc

#endif // DDC_ASSEMBLER_RDATAFRAMEASSEMBLER_H

