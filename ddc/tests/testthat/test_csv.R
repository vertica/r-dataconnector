library(ddc)

test_that("ex001.csv", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/ddc/test/data/ex001.csv',sep=''), options=list(schema='a:int64,b:string'))
  expect_equal(nrow(df), 3)
  expect_equal(ncol(df), 2)
})

test_that("ex002.csv", {
  df <- ddc_read(paste(getwd(),'/../../src/distributed-data-connector/ddc/ddc/test/data/ex002.csv',sep=''), options=list(schema='a:int64,b:string,c:int64,d:string'))
  expect_equal(nrow(df), 3)
  expect_equal(ncol(df), 4)
})

