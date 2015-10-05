BOOST_VERSION=1_58_0
ZMQ_VERSION=4.1.2
PROTOBUF_VERSION=2.6.1

INSTALLDIR=$(abspath install)
NCORES=$(shell getconf _NPROCESSORS_ONLN)

all: boost yajl curl openssl gtest gmock glog gflags z unwind lzma zmq protobuf snappy tcmalloc

boost: $(INSTALLDIR)/lib/libboost_chrono.a $(INSTALLDIR)/lib/libboost_date_time.a $(INSTALLDIR)/lib/libboost_filesystem.a  $(INSTALLDIR)/lib/libboost_system.a $(INSTALLDIR)/lib/libboost_thread.a

boost_$(BOOST_VERSION)/b2:
	tar xf boost_$(BOOST_VERSION).tar.gz
	cd boost_$(BOOST_VERSION); ./bootstrap.sh --with-libraries=thread,system,chrono

$(INSTALLDIR)/lib/libboost_chrono.a $(INSTALLDIR)/lib/libboost_date_time.a $(INSTALLDIR)/lib/libboost_filesystem.a $(INSTALLDIR)/lib/libboost_system.a $(INSTALLDIR)/lib/libboost_thread.a: boost_$(BOOST_VERSION)/b2
	cd boost_$(BOOST_VERSION); ./b2 -j$(NCORES) --with-filesystem --with-date_time --with-thread --with-chrono --with-system cxxflags=-fPIC link=static runtime-link=shared --stagedir=$(INSTALLDIR)/ stage


protobuf: $(INSTALLDIR)/lib/libprotobuf.a
$(INSTALLDIR)/lib/libprotobuf.a:
	tar -zxf protobuf-$(PROTOBUF_VERSION).tar.gz
	cd protobuf-$(PROTOBUF_VERSION); CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES) && make install

snappy: $(INSTALLDIR)/lib/libsnappy.a
$(INSTALLDIR)/lib/libsnappy.a:
	tar -zxf snappy-1.1.1.tar.gz
	cd snappy-1.1.1; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES) && make install

zmq: $(INSTALLDIR)/lib/libzmq.a
$(INSTALLDIR)/lib/libzmq.a:  $(INSTALLDIR)/lib/libuuid.a
	tar -zxf zeromq-$(ZMQ_VERSION).tar.gz
	mkdir -p $(INSTALLDIR)/include
	cp -rf zmq.hpp $(INSTALLDIR)/include
	cd zeromq-$(ZMQ_VERSION); CFLAGS="-fPIC -I$(INSTALLDIR)/include" CXXFLAGS="-fPIC -I$(INSTALLDIR)/include" LDFLAGS="-fPIC -L$(INSTALLDIR)/lib" ./configure --with-libsodium=no --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES) && make install

uuid: $(INSTALLDIR)/lib/libuuid.a
$(INSTALLDIR)/lib/libuuid.a:
	tar -zxf libuuid-1.0.3.tar.gz
	cd libuuid-1.0.3; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure  --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES) && make install

yajl:  $(INSTALLDIR)/lib/libyajl.a
 $(INSTALLDIR)/lib/libyajl.a: $(INSTALLDIR)/bin/cmake
	mkdir -p $(INSTALLDIR)/include
	tar -zxf yajl-2.1.0.tar.gz
	cd lloyd-yajl-66cb08c; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC PATH=$(INSTALLDIR)/bin:$(PATH) ./configure  --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES); cp -r ./build/yajl-2.1.0/include/yajl $(INSTALLDIR)/include; cp build/yajl-2.1.0/lib/libyajl_s.a $(INSTALLDIR)/lib/libyajl.a

curl:  $(INSTALLDIR)/lib/libcurl.a
$(INSTALLDIR)/lib/libcurl.a: $(INSTALLDIR)/lib/libssl.a $(INSTALLDIR)/lib/libz.a
	tar -zxf curl-7.43.0.tar.gz
	cd curl-7.43.0; CFLAGS="-fPIC" CXXFLAGS="-fPIC" LDFLAGS="-fPIC -Wl,--no-as-needed -ldl" ./configure --with-zlib=$(INSTALLDIR)  --with-ssl=$(INSTALLDIR)  --enable-static --enable-shared=no --prefix=$(INSTALLDIR) --disable-ldap --without-librtmp --without-libidn && make -j$(NCORES) && make install

openssl:  $(INSTALLDIR)/lib/libssl.a
$(INSTALLDIR)/lib/libssl.a:
	tar -zxf openssl-1.0.2c.tar.gz
	cd openssl-1.0.2c; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./config  shared --prefix=$(INSTALLDIR); make; cp libssl.a libcrypto.a $(INSTALLDIR)/lib && cp -Lr include/* $(INSTALLDIR)/include/

gtest:  $(INSTALLDIR)/lib/libgtest.a
$(INSTALLDIR)/lib/libgtest.a:  
	tar xf gtest-1.6.0.tar.gz
	cd gtest-1.6.0; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make  && cp lib/.libs/libgtest.a $(INSTALLDIR)/lib/; cp -rf include/* $(INSTALLDIR)/include/

gmock:  $(INSTALLDIR)/lib/libgmock.a
$(INSTALLDIR)/lib/libgmock.a:
	tar xf gmock-1.6.0.tar.gz
	cd gmock-1.6.0; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make  && cp lib/.libs/libgmock.a $(INSTALLDIR)/lib/; cp -rf include/* $(INSTALLDIR)/include/


glog:  $(INSTALLDIR)/lib/libglog.a 
$(INSTALLDIR)/lib/libglog.a:  $(INSTALLDIR)/lib/libgflags.a
	tar -zxf glog-0.3.3.tar.gz
	cd glog-0.3.3; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make  && make install

gflags:  $(INSTALLDIR)/lib/libgflags.a
$(INSTALLDIR)/lib/libgflags.a:  $(INSTALLDIR)/bin/cmake
	tar -zxf gflags-2.1.2.tar.gz
	mkdir -p gflags-2.1.2/Release
	cd gflags-2.1.2/Release; $(INSTALLDIR)/bin/cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" .. && make -j$(NCORES)  && cp -r include/gflags $(INSTALLDIR)/include && cp lib/libgflags.a $(INSTALLDIR)/lib


z:  $(INSTALLDIR)/lib/libz.a
$(INSTALLDIR)/lib/libz.a:
	tar -zxf zlib-1.2.8.tar.gz
	cd zlib-1.2.8; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --static --prefix=$(INSTALLDIR) && make -j$(NCORES)  && make install

unwind:  $(INSTALLDIR)/lib/libunwind.a
$(INSTALLDIR)/lib/libunwind.a:  $(INSTALLDIR)/lib/liblzma.a
	tar -zxf libunwind-1.1.tar.gz
	cd libunwind-1.1; CFLAGS="-fPIC -I$(INSTALLDIR)/include" CXXFLAGS="-fPIC -I$(INSTALLDIR)/include" LDFLAGS="-fPIC -L$(INSTALLDIR)/lib" ./configure  --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES)  && make install

lzma:  $(INSTALLDIR)/lib/liblzma.a
$(INSTALLDIR)/lib/liblzma.a:
	tar -zxf xz-5.2.1.tar.gz
	cd xz-5.2.1; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure  --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES)  && make install

cmake: $(INSTALLDIR)/bin/cmake
$(INSTALLDIR)/bin/cmake:
	tar -zxf cmake-3.2.3.tar.gz
	cd cmake-3.2.3; ./configure  --prefix=$(INSTALLDIR) && make -j$(NCORES)  && make -j$(NCORES) install

tcmalloc:$(INSTALLDIR)/lib/libtcmalloc.a
$(INSTALLDIR)/lib/libtcmalloc.a: $(INSTALLDIR)/lib/libunwind.a
	tar -zxf gperftools-2.4.tar.gz
	cd gperftools-2.4; CFLAGS=-fPIC CXXFLAGS=-fPIC LDFLAGS=-fPIC ./configure --enable-static --enable-shared=no --prefix=$(INSTALLDIR) && make -j$(NCORES)  && make -j$(NCORES) install

clean: 
	rm -rf $(INSTALLDIR)
	rm -rf boost_$(BOOST_VERSION)/
	rm -rf protobuf-2.5.0/
	rm -rf zeromq-4.0.4
	rm -rf libxml2-2.9.1
	rm -rf openssl-1.0.2c
	rm -rf curl-7.43.0
	rm -rf lloyd-yajl-66cb08c
	rm -rf gflags-2.1.2
	rm -rf zlib-1.2.8
	rm -rf libunwind-1.1
	rm -rf xz-5.2.1
	rm -rf cmake-3.2.3
	rm -rf glog-0.3.3
	rm -rf gtest-1.7.0
	rm -rf snappy-1.1.1
	rm -rf protobuf-2.6.1
	rm -rf zeromq-4.1.2
	rm -rf libuuid-1.0.3
	rm -rf gmock-1.7.0
	rm -rf gperftools-2.4


