### Introduction
Using the `hdfsconnector` package, R users can read CSV and ORC files from HDFS of the local file system. `hdfsconnector` is extensible and new file formats abd filesystems can be easily supported.

Supported formats:
* CSV
* ORC [link](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC)

Supported file systems:
* HDFS
* Local FS

### Examples
    library(hdfsconnector)

    # Load a CSV file from the local file system
    df <- csv2dataframe(url='/tmp/test.csv', schema='age:int64,name:string')

    # Load a CSV file from HDFS
    df <- csv2dataframe(url='hdfs:///test.csv', schema='age:int64,name:string')

    # Load an ORC file from HDFS
    df <- orc2dataframe(url='hdfs:///test.orc')

    # write a file to HDFS
    object2hdfs(mymodel, 'hdfs:///file.out', overwrite=1)

### Parameters
* `url`: File URL. Example: `hdfs:///file.csv`
* `schema`: CSV only. Format is `<col0-name>:<col0_type>,<col1-name>:<col1_type>,...<colN-name>:<colN_type>` (supported types are `int64`, `double` and `string`). Example: `schema='age:int64,name:string'`.
* `fileType`: file type is determined automatically by the file extension. Users can use `fileType` to override it. Useful when files don't have extensions. Example: `fileType='csv'`.
* `delimiter`: CSV only. Column separator. By default delimiter is `','`. Example: `delimiter='|'`.
* `selectedStripes`: ORC only. Which ORC stripes to include. By default all are included. Example: `selectedStripes='0,1,2'`.
* `hdfsConfigurationFile`: HDFS configuration file. By default `paste(system.file(package='ddc'),'/conf/hdfs.json',sep='')`. Parameters in this file are `port` (webhdfs port), `user` (HDFS) and `host` (namenode). An example file looks like this:

        {
            "port": 50070,
            "host": "172.17.0.3",
            "user":"jorgem"
        }

