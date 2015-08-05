library(hdfsconnector)

test_that("decimal.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/decimal.orc',sep=''))
  expect_equal(nrow(df), 6000)
  expect_equal(ncol(df), 1)
})


test_that("TestOrcFile.testDate2038.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testDate2038.orc',sep=''))
  expect_equal(nrow(df), 212000)
  expect_equal(ncol(df), 2)
})


test_that("demo-11-none.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/demo-11-none.orc',sep=''))
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.testMemoryManagementV11.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testMemoryManagementV11.orc',sep=''))
  expect_equal(nrow(df), 2500)
  expect_equal(ncol(df), 2)
})


test_that("demo-11-zlib.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/demo-11-zlib.orc',sep=''))
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.0testMemoryManagementV12.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testMemoryManagementV12.orc',sep=''))
  expect_equal(nrow(df), 2500)
  expect_equal(ncol(df), 2)
})


test_that("demo-12-zlib.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/demo-12-zlib.orc',sep=''))
  expect_equal(nrow(df), 1920800)
  expect_equal(ncol(df), 9)
})


test_that("TestOrcFile.testPredicatePushdown.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testPredicatePushdown.orc',sep=''))
  expect_equal(nrow(df), 3500)
  expect_equal(ncol(df), 2)
})


test_that("nulls-at-end-snappy.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/nulls-at-end-snappy.orc',sep=''))
  expect_equal(nrow(df), 70000)
  expect_equal(ncol(df), 7)
})


test_that("TestOrcFile.testSeek.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testSeek.orc',sep=''))
  expect_equal(nrow(df), 32768)
  expect_equal(ncol(df), 12)
})


#test_that("orc-file-11-format.orc", {
#  df <- hdfs_read(paste(getwd(),'/data/orc/orc-file-11-format.orc',sep=''))
#  expect_equal(nrow(df), 7500)
#  expect_equal(ncol(df), 14)
#})


test_that("TestOrcFile.testSnappy.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testSnappy.orc',sep=''))
  expect_equal(nrow(df), 10000)
  expect_equal(ncol(df), 2)
})


test_that("orc_split_elim.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/orc_split_elim.orc',sep=''))
  expect_equal(nrow(df), 25000)
  expect_equal(ncol(df), 5)
})


test_that("TestOrcFile.testStringAndBinaryStatistics.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testStringAndBinaryStatistics.orc',sep=''))
  expect_equal(nrow(df), 4)
  expect_equal(ncol(df), 2)
})


test_that("over1k_bloom.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/over1k_bloom.orc',sep=''))
  expect_equal(nrow(df), 2098)
  expect_equal(ncol(df), 11)
})


test_that("TestOrcFile.testStripeLevelStats.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testStripeLevelStats.orc',sep=''))
  expect_equal(nrow(df), 11000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.columnProjection.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.columnProjection.orc',sep=''))
  expect_equal(nrow(df), 21000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.testTimestamp.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testTimestamp.orc',sep=''))
  expect_equal(nrow(df), 12)
  expect_equal(ncol(df), 1)
})


test_that("TestOrcFile.emptyFile.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.emptyFile.orc',sep=''))
  expect_equal(nrow(df), 0)
  expect_equal(ncol(df), 12)
})


test_that("TestOrcFile.testUnionAndTimestamp.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testUnionAndTimestamp.orc',sep=''))
  expect_equal(nrow(df), 5077)
  expect_equal(ncol(df), 3)
})


test_that("TestOrcFile.metaData.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.metaData.orc',sep=''))
  expect_equal(nrow(df), 1)
  expect_equal(ncol(df), 12)
})


test_that("TestOrcFile.testWithoutIndex.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testWithoutIndex.orc',sep=''))
  expect_equal(nrow(df), 50000)
  expect_equal(ncol(df), 2)
})


test_that("TestOrcFile.test1.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.test1.orc',sep=''))
  expect_equal(nrow(df), 2)
  expect_equal(ncol(df), 12)
})


test_that("version1999.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/version1999.orc',sep=''))
  expect_equal(nrow(df), 0)
  expect_equal(ncol(df), 0)
})


test_that("TestOrcFile.testDate1900.orc", {
  df <- hdfs_read(paste(getwd(),'/data/orc/TestOrcFile.testDate1900.orc',sep=''))
  expect_equal(nrow(df), 70000)
  expect_equal(ncol(df), 2)
})

