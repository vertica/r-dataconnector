cmake_minimum_required(VERSION 2.6)
project(ddc)

cmake_policy(SET CMP0015 NEW)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set (CMAKE_CXX_FLAGS "-g -O2")
  set (CXX11_FLAGS "-std=c++11")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Wno-unknown-pragmas")
  set (WARN_FLAGS "${WARN_FLAGS} -Wconversion -Werror")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
#  set (CMAKE_CXX_FLAGS "-g -O2")
  set (WARN_FLAGS "-Wall -Wno-unknown-pragmas -Wconversion -Werror")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.3")
    set (CXX11_FLAGS "")
    set (WARN_FLAGS "-Wformat -Werror")
  elseif (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.7")
    set (CXX11_FLAGS "-std=c++0x")
  else ()
    set (CXX11_FLAGS "-std=c++11")
  endif ()
elseif (MSVC)
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  # TODO: We assume MSVC debug mode. In the future, set these flags
  # appropriately for all build mode.
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /MTd")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libc.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libcmt.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:msvcrt.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libcd.lib")

  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:msvcrtd.lib")
endif ()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")

# R packages
execute_process(COMMAND Rscript -e "Rcpp:::CxxFlags()" RESULT_VARIABLE rcpp_cxxflags_res OUTPUT_VARIABLE rcpp_inc_dir)
execute_process(COMMAND Rscript -e "RInside:::CxxFlags()" RESULT_VARIABLE rinside_cxxflags_res OUTPUT_VARIABLE rinside_inc_dir)
execute_process(COMMAND Rscript -e "Rcpp:::LdFlags()" RESULT_VARIABLE rcpp_ldflags_res OUTPUT_VARIABLE rcpp_ldflags)
execute_process(COMMAND Rscript -e "RInside:::LdFlags()" RESULT_VARIABLE rinside_ldflags_res OUTPUT_VARIABLE rinside_ldflags)

execute_process(COMMAND Rscript -e "cat(R.home('include'))" RESULT_VARIABLE r_incdir_res OUTPUT_VARIABLE r_incdir)
execute_process(COMMAND Rscript -e "cat(R.home('home'), '/lib', sep='')" RESULT_VARIABLE r_libdir_res OUTPUT_VARIABLE r_libdir)

IF((NOT ${rcpp_cxxflags_res} EQUAL 0) OR
    (NOT ${rinside_cxxflags_res} EQUAL 0) OR
    (NOT ${rcpp_ldflags_res} EQUAL 0) OR
    (NOT ${rinside_ldflags_res} EQUAL 0) OR
    (NOT ${r_incdir_res} EQUAL 0) OR 
    (NOT ${r_libdir_res} EQUAL 0))
    MESSAGE( FATAL_ERROR "R dependencies are not configured correctly. Make sure you have R, Rcpp and RInside installed.\nTo install Rcpp run 'R CMD INSTALL third-party/Rcpp_0.11.6.tar.gz'.\nTo install RInside run 'R CMD INSTALL third-party/RInside_0.2.13.tar.gz'")
ENDIF()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} -fPIC ${rcpp_inc_dir} ${rinside_inc_dir}")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}  ${rinside_ldflags}")

IF(CMAKE_BUILD_TYPE MATCHES Debug)
    SET(orc_inc_dir recordparser/orc/Debug/c++/include/ recordparser/orc/Debug/c++/src/)
    SET(orc_src_dir recordparser/orc/Debug/c++/src/*.cc)
ELSE(CMAKE_BUILD_TYPE MATCHES Debug)
    SET(orc_inc_dir recordparser/orc/Release/c++/include/ recordparser/orc/Release/c++/src/)
    SET(orc_src_dir recordparser/orc/Release/c++/src/*.cc)
ENDIF(CMAKE_BUILD_TYPE MATCHES Debug)

include_directories(./ assembler/ assembler/src
                    base/ base/src
                    blockreader/ blockreader/src
                    ddc/ ddc/src
                    distributor/ distributor/src
                    recordparser/ recordparser/src
                        recordparser/orc/c++/include/ recordparser/orc/c++/src/
                        ${orc_inc_dir}
                        recordparser/orc/c++/libs/snappy-1.1.2/
                        recordparser/text-csv/include/
                    splitproducer/ splitproducer/src
                    hdfsutils/webhdfs/ hdfsutils hdfsutils/src
                    worker/ worker/src  scheduler/ scheduler/src
                    third-party/install/include
                    third-party/boost_1_58_0)

include_directories (${r_incdir})

link_directories(${r_libdir}
                 third-party/install/lib)

FILE(GLOB assembler_sources assembler/src/*.cpp)
FILE(GLOB base_sources base/src/*.cpp)
FILE(GLOB blockreader_sources blockreader/src/*.cpp)
FILE(GLOB hdfsutils_sources hdfsutils/src/*.cpp)
FILE(GLOB ddc_sources ddc/src/*.cpp)
FILE(GLOB recordparser_sources recordparser/src/*.cpp)
FILE(GLOB splitproducer_sources splitproducer/src/*.cpp)
FILE(GLOB distributor_sources distributor/src/*.cpp)
FILE(GLOB worker_sources worker/src/*.cpp)
FILE(GLOB orc_sources recordparser/orc/c++/src/orc/*.cc ${orc_src_dir} )
FILE(GLOB scheduler_sources scheduler/src/*.cpp)

#webhdfs
SET(webhdfs_sources hdfsutils/webhdfs/webhdfs/buffer.c
                    hdfsutils/webhdfs/webhdfs/file.c
                    hdfsutils/webhdfs/webhdfs/webhdfs.c
                    hdfsutils/webhdfs/webhdfs/config.c
                    hdfsutils/webhdfs/webhdfs/request.c
                    hdfsutils/webhdfs/webhdfs/dir.c
                    hdfsutils/webhdfs/webhdfs/response.c)
SET_SOURCE_FILES_PROPERTIES( ${webhdfs_sources} PROPERTIES LANGUAGE CXX )
#ADD_LIBRARY(webhdfs SHARED ${webhdfs_sources})
ADD_LIBRARY(webhdfs_s STATIC ${webhdfs_sources})

ADD_LIBRARY(orc_s STATIC ${orc_sources})

SET(all_sources ${assembler_sources}
                ${blockreader_sources}
                ${ddc_sources}
                ${recordparser_sources}
                ${splitproducer_sources})

#ADD_LIBRARY(ddc SHARED ${all_sources})
ADD_LIBRARY(ddc_s STATIC ${all_sources})

#ADD_LIBRARY(ddcmaster SHARED ${scheduler_sources})
ADD_LIBRARY(ddcmaster_s STATIC ${scheduler_sources} ddc/src/globals.cpp)
#ADD_LIBRARY(hdfsutils SHARED ${hdfsutils_sources})
ADD_LIBRARY(hdfsutils_s STATIC ${hdfsutils_sources})
#ADD_LIBRARY(base SHARED ${base_sources})
ADD_LIBRARY(base_s STATIC ${base_sources})

SET(ddc_dep_libs ddc_s hdfsutils_s webhdfs_s base_s orc_s
    gtest gmock R RInside glog gflags yajl curl ssl crypto z
    protobuf snappy unwind unwind-x86_64 lzma
    boost_thread pthread boost_system dl)
# tcmalloc

# tests
SET(test_sources
        assembler/test/assembler_test.cpp
        recordparser/test/recordparser_test.cpp
        recordparser/test/csvrecordparser_test.cpp
        recordparser/test/textcsv_test.cpp
        splitproducer/test/splitproducer_test.cpp
        blockreader/test/block_test.cpp
        blockreader/test/blockreader_test.cpp
        blockreader/test/hdfsblockreader_test.cpp
        blockreader/test/prefetchblockreader_test.cpp
        hdfsutils/test/hdfsblocklocator_test.cpp
        hdfsutils/test/basicurldownloader_test.cpp
        hdfsutils/test/hdfsfile_test.cpp
        hdfsutils/test/hdfsutils_test.cpp
        ddc/test/ddc_test.cpp
        ddc/test/driver.cpp)

SET(ddcmaster_test_sources
        scheduler/test/chunkscheduler_test.cpp
        ddc/test/driver.cpp ddc/src/globals.cpp)

ADD_EXECUTABLE(ddc_test ${test_sources})
TARGET_LINK_LIBRARIES(ddc_test ${ddc_dep_libs} ddcmaster_s)

ADD_EXECUTABLE(ddcmaster_test ${ddcmaster_test_sources})
TARGET_LINK_LIBRARIES(ddcmaster_test ddcmaster_s ${ddc_dep_libs})


#benchmark
#ADD_EXECUTABLE(blockreader_benchmark blockreader/test/blockreader_benchmark.cpp)
#TARGET_LINK_LIBRARIES(blockreader_benchmark ${ddc_dep_libs} folly follybenchmark)
#ADD_EXECUTABLE(splitproducer_benchmark splitproducer/test/splitproducer_benchmark.cpp)
#TARGET_LINK_LIBRARIES(splitproducer_benchmark ${ddc_dep_libs} folly follybenchmark)
#ADD_EXECUTABLE(recordparser_benchmark recordparser/test/recordparser_benchmark.cpp)
#TARGET_LINK_LIBRARIES(recordparser_benchmark ${ddc_dep_libs} folly follybenchmark)
#ADD_EXECUTABLE(assembler_benchmark assembler/test/assembler_benchmark.cpp)
#TARGET_LINK_LIBRARIES(assembler_benchmark ${ddc_dep_libs} folly follybenchmark)

#distributor
#ADD_LIBRARY(distributor SHARED ${distributor_sources} ${base_sources})
ADD_LIBRARY(distributor_s STATIC ${distributor_sources} ${base_sources})

SET(distributor_dep_libs distributor_s zmq protobuf boost_thread pthread boost_system
    glog gflags gtest unwind unwind-x86_64 lzma)

ADD_EXECUTABLE(requestreceiver_test distributor/test/requestreceiver_test.cpp)
TARGET_LINK_LIBRARIES(requestreceiver_test ${distributor_dep_libs})
ADD_EXECUTABLE(requestsender_test distributor/test/requestsender_test.cpp)
TARGET_LINK_LIBRARIES(requestsender_test ${distributor_dep_libs})

ADD_EXECUTABLE(master_test distributor/test/master_test.cpp)
TARGET_LINK_LIBRARIES(master_test ${distributor_dep_libs})

#worker
ADD_EXECUTABLE(worker_test ${worker_sources} worker/test/worker_test.cpp)
TARGET_LINK_LIBRARIES(worker_test ${ddc_dep_libs} ${distributor_dep_libs} ddcmaster_s)
