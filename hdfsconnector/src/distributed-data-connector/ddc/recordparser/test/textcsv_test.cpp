#include <iostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <boost/any.hpp>
#include <boost/variant.hpp>
#include <gtest/gtest.h>
#include "text/csv/iterator.hpp"
#include "text/csv/rows.hpp"

using namespace std;

namespace ddc{
namespace recordparser{
namespace testing{


// The fixture for testing class Foo.
class TextCsvTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  TextCsvTest() {
    // You can do set-up work for each test here.
  }

  virtual ~TextCsvTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(TextCsvTest, Iterate) {
    std::istringstream ss("1,2,3\n4,5,6");
    text::csv::csv_istream is(ss);
    text::csv::input_column_iterator<int> it(is);
    for (int i = 0; i < 3; ++i) {
        std::cout << *it << std::endl;
        ++it;
    }
    text::csv::input_column_iterator<int> it2(is);
    for (int i = 0; i < 3; ++i) {
        std::cout << *it2 << std::endl;
        ++it2;
    }
}

TEST_F(TextCsvTest, Row) {
    std::istringstream ss("1,2,3\n4,5,6");
    text::csv::csv_istream is(ss);
    text::csv::row row;
    while(!is.eof()) {
        is >> row;
        for(uint64_t i = 0; i < row.size(); i++) {
            std::cout << row[i] << std::endl;
        }
    }
}

bool is_int(const boost::any & operand)
{
  return operand.type() == typeid(int);
}
bool is_string(const boost::any & operand)
{
  return operand.type() == typeid(std::string);
}
bool is_double(const boost::any & operand)
{
  return operand.type() == typeid(double);
}

TEST_F(TextCsvTest, RowMap) {
    std::istringstream ss("jorge,0,10.0\nmanolo,1,5.0");
    text::csv::csv_istream is(ss);
    const std::size_t n = 3;
    text::csv::row names(n);
    names[0] = "name";
    names[1] = "id";
    names[2] = "score";

    text::csv::header h(names);
    text::csv::map_row row(h);
    while(!is.eof()) {
        is >> row;
        for(uint64_t i = 0; i < row.size(); i++) {
            std::cout << row[i] << " " << row[names[i]] <<
                         " is_int: " << is_int(boost::any(row[i])) <<
                         " is_string: " << is_string(boost::any(row[i])) <<
                         " is_double: " << is_double(boost::any(row[i])) <<
                         std::endl;
        }
    }
}



TEST_F(TextCsvTest, RowMapTypesAny) {
    std::istringstream ss("jorge,0,10.0\nmanolo,1,5.0");
    text::csv::csv_istream is(ss);
    const std::size_t n = 3;
    text::csv::row names(n);
    names[0] = "name";
    names[1] = "id";
    names[2] = "score";

    double d = 0.0;
    const std::type_info& doubleType = typeid(d);
    int i = 0;
    const std::type_info& intType = typeid(i);
    std::string s = "hola";
    const std::type_info& stringType = typeid(s);

    text::csv::header h(names);
    text::csv::map_row row(h);
    while(!is.eof()) {
        is >> row;
        for(uint64_t i = 0; i < row.size(); i++) {
            boost::any myany;
            if(i == 0) myany = row.as<std::string>(row[i]);
            else if(i == 1) myany = row.as<int>(row[i]);
            else if(i == 2) myany = row.as<double>(row[i]);
            std::cout << row[i] << " " << row[names[i]] <<
                         " is_int: " << is_int(myany) <<
                         " is_string: " << is_string(myany) <<
                         " is_double: " << is_double(myany) <<
                         std::endl;
        }
    }
}

typedef boost::variant<std::string, int, double> MyVariant;

void whichVariant(MyVariant &v) {
    switch (v.which()) {
        case 0:
        {
            std::cout << "it's an string" << std::endl;
            break;
        }
        case 1:
        {
            std::cout << "it's an int" << std::endl;
            break;
        }
        case 2:
        {
            std::cout << "it's a double" << std::endl;
            break;
        }
        default:
        {
            throw runtime_error("don't know what it is");
            break;
        }
    }
}

TEST_F(TextCsvTest, RowMapTypesVariant) {
    std::istringstream ss("jorge,0,10.0\nmanolo,1,5.0");
    text::csv::csv_istream is(ss);
    const std::size_t n = 3;
    text::csv::row names(n);
    names[0] = "name";
    names[1] = "id";
    names[2] = "score";

    double d = 0.0;
    const std::type_info& doubleType = typeid(d);
    int i = 0;
    const std::type_info& intType = typeid(i);
    std::string s = "hola";
    const std::type_info& stringType = typeid(s);

    text::csv::header h(names);
    text::csv::map_row row(h);
    while(!is.eof()) {
        is >> row;
        for(uint64_t i = 0; i < row.size(); i++) {
            MyVariant v;
            if(i == 0) v = row.as<std::string>(row[i]);
            else if(i == 1) v = row.as<int>(row[i]);
            else if(i == 2) v = row.as<double>(row[i]);
            std::cout << row[i] << " " << row[names[i]] << std::endl;
            whichVariant(v);
        }
    }
}

TEST_F(TextCsvTest, Empty) {
    std::istringstream ss("");
    text::csv::csv_istream is(ss);
    std::cout << "is eof: " << is.eof() << std::endl;

    std::istringstream ss2("hola");
    text::csv::csv_istream is2(ss2);
    std::cout << "is eof: " << is2.eof() << std::endl;
}

} // namespace testing
} // namespace recordparser
} // namespace ddc
