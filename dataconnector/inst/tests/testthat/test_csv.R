library(hdfsconnector)

test_that("ex001.csv", {
  df <- hdfs_read(paste(getwd(),'/data/csv/ex001.csv',sep=''), schema='a:int64,b:string')
  expect_equal(nrow(df), 3)
  expect_equal(ncol(df), 2)
})

test_that("ex002.csv", {
  df <- hdfs_read(paste(getwd(),'/data/csv/ex002.csv',sep=''), schema='a:int64,b:string,c:int64,d:string')
  expect_equal(nrow(df), 3)
  expect_equal(ncol(df), 4)
})

