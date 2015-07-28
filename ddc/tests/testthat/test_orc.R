library(ddc)
test_that("decimal.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/decimal.orc',sep=''), options=list())
  expect_equal(nrow(df), 6000)
  expect_equal(ncol(df), 1)
})


test_that("TestOrcFile.testDate2038.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testDate2038.orc',sep=''), options=list())
  expect_equal(nrow(df), 212000)
  expect_equal(ncol(df), 2)
})


test_that("demo-11-none.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/demo-11-none.orc',sep=''), options=list())
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.testMemoryManagementV11.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testMemoryManagementV11.orc',sep=''), options=list())
  expect_equal(nrow(df), 2500)
  expect_equal(ncol(df), 2)
})


test_that("demo-11-zlib.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/demo-11-zlib.orc',sep=''), options=list())
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.0testMemoryManagementV12.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testMemoryManagementV12.orc',sep=''), options=list())
  expect_equal(nrow(df), 2500)
  expect_equal(ncol(df), 2)
})


test_that("demo-12-zlib.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/demo-12-zlib.orc',sep=''), options=list())
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.testPredicatePushdown.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testPredicatePushdown.orc',sep=''), options=list())
  expect_equal(nrow(df), 3500)
  expect_equal(ncol(df), 2)
})


test_that("nulls-at-end-snappy.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/nulls-at-end-snappy.orc',sep=''), options=list())
  expect_equal(nrow(df), 70000)
  expect_equal(ncol(df), 7)
})


test_that("TestOrcFile.testSeek.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testSeek.orc',sep=''), options=list())
  expect_equal(nrow(df), 32768)
  expect_equal(ncol(df), 12)
})


#test_that("orc-file-11-format.orc", {
#  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/orc-file-11-format.orc',sep=''), options=list())
#  expect_equal(nrow(df), 7500)
#  expect_equal(ncol(df), 14)
#})


test_that("TestOrcFile.testSnappy.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testSnappy.orc',sep=''), options=list())
  expect_equal(nrow(df), 10000)
  expect_equal(ncol(df), 2)
})


test_that("orc_split_elim.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/orc_split_elim.orc',sep=''), options=list())
  expect_equal(nrow(df), 25000)
  expect_equal(ncol(df), 5)
})


test_that("TestOrcFile.testStringAndBinaryStatistics.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testStringAndBinaryStatistics.orc',sep=''), options=list())
  expect_equal(nrow(df), 4)
  expect_equal(ncol(df), 2)
})


test_that("over1k_bloom.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/over1k_bloom.orc',sep=''), options=list())
  expect_equal(nrow(df), 2098)
  expect_equal(ncol(df), 11)
})


test_that("TestOrcFile.testStripeLevelStats.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testStripeLevelStats.orc',sep=''), options=list())
  expect_equal(nrow(df), 11000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.columnProjection.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.columnProjection.orc',sep=''), options=list())
  expect_equal(nrow(df), 21000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.testTimestamp.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testTimestamp.orc',sep=''), options=list())
  expect_equal(nrow(df), 12)
  expect_equal(ncol(df), 1)
})


test_that("TestOrcFile.emptyFile.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.emptyFile.orc',sep=''), options=list())
  expect_equal(nrow(df), 0)
  expect_equal(ncol(df), 12)
})


test_that("TestOrcFile.testUnionAndTimestamp.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testUnionAndTimestamp.orc',sep=''), options=list())
  expect_equal(nrow(df), 5077)
  expect_equal(ncol(df), 3)
})


test_that("TestOrcFile.metaData.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.metaData.orc',sep=''), options=list())
  expect_equal(nrow(df), 1)
  expect_equal(ncol(df), 12)
})


test_that("TestOrcFile.testWithoutIndex.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testWithoutIndex.orc',sep=''), options=list())
  expect_equal(nrow(df), 50000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.test1.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.test1.orc',sep=''), options=list())
  expect_equal(nrow(df), 2)
  expect_equal(ncol(df), 12)
})


test_that("version1999.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/version1999.orc',sep=''), options=list())
  expect_equal(nrow(df), 0)
  expect_equal(ncol(df), 0)
})


test_that("TestOrcFile.testDate1900.orc", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/recordparser/orc/examples/TestOrcFile.testDate1900.orc',sep=''), options=list())
  expect_equal(nrow(df), 70000)
  expect_equal(ncol(df), 2)
})

