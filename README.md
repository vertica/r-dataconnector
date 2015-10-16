### Introduction

The `dataconnector` package allows R users to read CSV and ORC files from HDFS and the local file system. 

The package is extensible and new file formats and file systems can be added easily.

It can be used together with [Distributed R](https://github.com/vertica/distributedr) to distribute the file loading across cores and machines.

Supported formats:
* CSV
* [ORC](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC)

Supported file systems:
* HDFS
* Local FS

### Installation

    $ git clone --recursive https://github.com/vertica/r-dataconnector.git 
    $ R CMD INSTALL r-dataconnector/dataconnector

### Examples

    library(dataconnector)

    # Load a CSV file from the local file system
    df <- csv2dataframe(url='/tmp/test.csv', schema='age:int64,name:string')

    # Load a CSV file from HDFS
    df <- csv2dataframe(url='hdfs:///test.csv', schema='age:int64,name:string')

    # Load an ORC file from HDFS
    df <- orc2dataframe(url='hdfs:///test.orc')

    # write a file to HDFS
    object2hdfs(mymodel, 'hdfs:///file.out', overwrite=1)

### Usage

    R> ?csv2dataframe
    R> ?orc2dataframe

### License

Apache 2.0.

