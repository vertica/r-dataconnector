
#ifndef DDC_RECORDPARSER_ORCRECORDPARSER_H_
#define DDC_RECORDPARSER_ORCRECORDPARSER_H_

#include <boost/any.hpp>
#include <boost/variant.hpp>

#include "irecordparser.h"
#include "hdfsutils/hdfsinputstream.h"
#include "orc/ColumnPrinter.hh"
#include "orc/Reader.hh"

namespace ddc {
namespace recordparser {


typedef boost::variant<bool, int32_t, int64_t, double, std::string> OrcPrimitive;

struct Node {
    Node()
    {
    }

    bool isNull;
    orc::TypeKind type;
    OrcPrimitive value;
    std::vector<boost::shared_ptr<Node> > elements;  // for lists and structs
    std::vector<std::string> fieldNames;  // key names
    std::vector<std::pair<boost::shared_ptr<Node> ,boost::shared_ptr<Node> > > kvpairs;  // for maps
};
typedef boost::shared_ptr<Node> NodePtr;

std::ostream &operator<<(std::ostream &stream, const Node& node);

void traverse(NodePtr node);
void traverse(NodePtr node, int level);

class OrcRecordParser : public IRecordParser {
 public:
    OrcRecordParser();
    ~OrcRecordParser();

    void configure(base::ConfigurationMap &conf);

    bool hasNext();
    boost::any next();

    uint64_t nullCount(uint64_t colIndex) {
        return nullCount_[colIndex];
    }
    uint64_t cols() const {
        return numColumns_;
    }
    uint64_t topCols() const {
        return numTopColumns_;
    }

    uint64_t rows() const {
        return numRows_;
    }

 private:

    void getRecord(orc::ColumnVectorBatch* column,
                                    orc::TypeKind kind,
                                    uint64_t rowId,
                                    uint64_t nestedLevel,
                                    NodePtr record,
                                    NodePtr parent);

    void initializeStripe();

    void getColNames(const orc::Type& type);

    void getColNames(const orc::Type& type,
                                      int level);

    std::string getColName(uint64_t index) const {
        if(index >= colnames_.size()) {
            throw std::runtime_error("out of bounds");
        }
        return colnames_[index];
    }

    bool configured_;
    std::unique_ptr<orc::Reader> orcReader_;
    std::unique_ptr<orc::ColumnVectorBatch> batch_;
    orc::ColumnVectorBatch* currentColumn_;
    std::string url_;
    uint64_t numStripes_;
    uint64_t numColumns_;
    uint64_t numTopColumns_;
    uint64_t numRows_;
    uint64_t numRowsInThisStripe_;
    uint64_t rowIndex_;
    uint64_t colIndex_; // fine-grained (e.g. a struct column has many child columns)
    uint64_t topColIndex_; //only top level cols
    uint64_t stripeIndex_;
    uint64_t colsInLastComplexType_;
    std::vector<orc::TypeKind> columnKinds_;

    // count how many nulls on each column
    std::map<uint64_t, uint64_t> nullCount_;

    // which stripes we have to read
    std::vector<uint64_t> selectedStripes_;

    std::vector<uint64_t> topIndexes_;

    // contains starting row of each selected stripe
    std::vector<uint64_t> stripeRowOffsets_;
    std::vector<uint64_t> stripeNumRows_;

    std::vector<std::string> colnames_;
    uint64_t colNameIndex_;
    std::vector<uint64_t> colIndexNameResetPoints_;

};

}  // namespace recordparser
}  // namespace ddc

#endif // DDC_RECORDPARSER_ORCRECORDPARSER_H_
