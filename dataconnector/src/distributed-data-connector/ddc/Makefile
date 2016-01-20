NCORES=$(shell getconf _NPROCESSORS_ONLN)
BASE=$(abspath ./)
BUILD_TYPE=Release
#BUILD_TYPE=Debug

.PHONY: third-party orc ddc clean

all: third-party orc ddc

# Undefine OBJECTS to avoid openssl compilation failure
third-party:
	mkdir -p $(BASE)/third-party/install/include
	mkdir -p $(BASE)/third-party/install/bin
	mkdir -p $(BASE)/third-party/install/lib
	cd $(BASE)/third-party/; make OBJECTS=''

orc: third-party
	mkdir -p $(BASE)/recordparser/orc/$(BUILD_TYPE); cd $(BASE)/recordparser/orc/$(BUILD_TYPE); cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) .. && make -j$(NCORES) orc

ddc: third-party orc
	mkdir -p $(BASE)/$(BUILD_TYPE); cd $(BASE)/$(BUILD_TYPE); cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) .. && make -j$(NCORES) ddc_s ddcmaster_s hdfsutils_s base_s webhdfs_s orc_s && cp libddc_s.a libddcmaster_s.a libhdfsutils_s.a libbase_s.a libwebhdfs_s.a liborc_s.a $(BASE)/third-party/install/lib

clean:
	cd $(BASE)/third-party/; make clean
	cd $(BASE)/recordparser/orc/$(BUILD_TYPE); make clean
	cd $(BASE)/$(BUILD_TYPE); make clean
