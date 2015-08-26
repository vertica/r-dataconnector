#include <glog/logging.h>
#include <gtest/gtest.h>
#include <RInside.h>

namespace google {
namespace protobuf  {
void ShutdownProtobufLibrary();
}
}

int main(int argc, char **argv) {
    RInside R(argc, argv);
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    int res =  RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return res;
}

