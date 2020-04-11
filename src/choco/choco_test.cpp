#include "gtest/gtest.h"

#include "common.h"

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();
    google::LogToStderr();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
