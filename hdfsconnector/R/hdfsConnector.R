#' \pkg{hdfsconnector} allows users to read and write files from HDFS.
#'
#' \pkg{hdfsconnector} is extensible and new file formats and filesystems can be easily added. So far it supports:
#'
#' Formats:
#' \itemize{
#'   \item CSV
#'   \item ORC
#' }
#' Filesystems:
#' \itemize{
#'   \item HDFS
#'   \item Local FS
#' }
#'
#' @docType package
#' @name hdfsconnector
NULL

#' Load a CSV file into a data frame.
#'
#' @param url File URL. Examples: '/tmp/file.csv', 'hdfs:///file.csv'.
#'
#'                      We also support globbing. Examples: '/tmp/*.csv', 'hdfs:///tmp/*.csv'.
#'
#'                      When globbing all CSV files need to have the same schema and delimiter.
#' @param schema  Specifies the column names and types.
#'
#'                Syntax is: \code{<col0-name>:<col0_type>,<col1-name>:<col1_type>,...<colN-name>:<colN_type>}.
#'
#'                Supported types are: \code{logical}, \code{integer}, \code{int64}, \code{numeric} and \code{character}. 
#'
#'                Example: schema='age:int64,name:character'.
#'
#'                Note that due to R not having a proper int64 type we convert it to an R numeric. Type conversion work as follows:
#'
#'                \tabular{ll}{
#'                    CSV type  |\tab R type    \cr
#'                    -         |\tab -         \cr
#'                    integer   |\tab integer   \cr
#'                    numeric   |\tab numeric   \cr
#'                    logical   |\tab logical   \cr
#'                    int64     |\tab numeric   \cr
#'                    character |\tab character
#'                }
#' @param delimiter Column separator. Example: delimiter='|'. By default delimiter is ','.
#' @param commentCharacter Discard lines starting with this character. Leading spaces are ignored.
#' @param hdfsConfigurationFile By default: \code{paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')}.
#'
#'                              Options are:
#'                              \itemize{
#'                                  \item webhdfsPort: webhdfs port, integer
#'                                  \item hdfsPort: hdfs namenode port, integer
#'                                  \item hdfsHost: hdfs namenode host, string
#'                                  \item hdfsUser: hdfs username, string
#'                              }
#'
#'                              An example file is:
#'
#'                                  \{ \cr
#'                                  "webhdfsPort": 50070, \cr
#'                                  "hdfsPort": 9000, \cr
#'                                  "hdfsHost": "172.17.0.3", \cr
#'                                  "hdfsUser": "jorgem" \cr
#'                                  \}
#'
#' @param hdfsConfigurationStr HDFS configuration in string format. Useful for Distributed R. 
#'                             Normally users specify hdfsConfigurationFile instead of hdfsConfigurationStr.
#' @param chunkStart Byte offset in the file to start parsing. Defaults to 0.
#' @param chunkEnd Byte offset in the file to stop parsing. Defaults to file size.
#' @param skipHeader Treat first line as the CSV header and discard it.
#' @return A dataframe representing the CSV file.
#' @examples
#' df <- csv2dataframe(url=paste(system.file(package='hdfsconnector'),'/tests/testthat/data/csv/ex001.csv',sep=''), schema='a:int64,b:string')

csv2dataframe <- function(url, schema, delimiter=',', commentCharacter='#',
                          hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep=''),
                          hdfsConfigurationStr='',
                          chunkStart=0, chunkEnd=-1,
                          skipHeader=FALSE) {
    options = list()
    options['schema'] = schema
    options['delimiter'] = delimiter
    options['commentCharacter'] = commentCharacter
    options['hdfsConfigurationFile'] = hdfsConfigurationFile
    if (hdfsConfigurationStr != '') {
        options['hdfsConfigurationStr'] = hdfsConfigurationStr
    }
    options['chunkStart'] = chunkStart
    options['chunkEnd'] = chunkEnd
    options['skipHeader'] = skipHeader

#    if(!("hdfsConfigurationFile" %in% names(options))) {
#        # set default hdfsConfigurationFile
#        options["hdfsConfigurationFile"] = paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')
#    }

    tmpfilename <- ''
    if(("hdfsConfigurationStr" %in% names(options))) {
        tmpfilename <- tempfile(pattern = "hdfs_", tmpdir = '/tmp', fileext = ".json")
        f <- file(tmpfilename)
        writeLines(as.character(options["hdfsConfigurationStr"]),f)
        close(f)
        options["hdfsConfigurationFile"] = tmpfilename
    }

    options['fileType'] = 'csv'

    df <- tryCatch({
            ddc_read(url, options)
        }, error=function(e) {
            stop(e)
        }, finally = {
            if(("hdfsConfigurationStr" %in% names(options))) {
                file.remove(tmpfilename)
            }
        })
    df
}

#' Load an ORC file into a data frame
#'
#' Type conversions work as follows:
#'                \tabular{ll}{
#'                    ORC type          |\tab R type        \cr
#'                    -                 |\tab -             \cr
#'                    byte/short/int    |\tab integer       \cr
#'                    float/double      |\tab numeric       \cr
#'                    long              |\tab numeric       \cr
#'                    bool              |\tab logical       \cr
#'                    string/binary     |\tab character     \cr
#'                    char/varchar      |\tab character     \cr
#'                    decimal           |\tab character     \cr
#'                    timestamp/date    |\tab character     \cr
#'                    union             |\tab not supported \cr
#'                    struct            |\tab dataframe     \cr
#'                    map               |\tab dataframe     \cr
#'                    list              |\tab list
#'                }
#'
#' @param url File URL. Examples: '/tmp/file.orc', 'hdfs:///file.orc'.
#' @param selectStripes ORC stripes to include. Stripes need to be consecutive.
#' @param hdfsConfigurationFile By default: \code{paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')}.
#'
#'                              Options are:
#'                              \itemize{
#'                                  \item webhdfsPort: webhdfs port, integer
#'                                  \item hdfsPort: hdfs namenode port, integer
#'                                  \item hdfsHost: hdfs namenode host, string
#'                                  \item hdfsUser: hdfs username, string
#'                              }
#'
#'                              An example file is:
#'
#'                                  \{ \cr
#'                                  "webhdfsPort": 50070, \cr
#'                                  "hdfsPort": 9000, \cr
#'                                  "hdfsHost": "172.17.0.3", \cr
#'                                  "hdfsUser": "jorgem" \cr
#'                                  \}
#' @param hdfsConfigurationStr HDFS configuration in string format. Useful for Distributed R. 
#'                             Normally users specify hdfsConfigurationFile instead of hdfsConfigurationStr.
#' @return A dataframe representing the ORC file.
#' @examples
#' df <- orc2dataframe(url=paste(system.file(package='hdfsconnector'),'/tests/testthat/data/orc/TestOrcFile.test1.orc',sep=''))

orc2dataframe <- function(url, selectedStripes='',
                          hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep=''),
                          hdfsConfigurationStr='' ) {
    options = list()
    options['selectedStripes'] = selectedStripes
    options['hdfsConfigurationFile'] = hdfsConfigurationFile
    if (hdfsConfigurationStr != '') {
        options['hdfsConfigurationStr'] = hdfsConfigurationStr
    }

#    if(!("hdfsConfigurationFile" %in% names(options))) {
#        # set default hdfsConfigurationFile
#        options["hdfsConfigurationFile"] = paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')
#    }

    tmpfilename <- ''
    if("hdfsConfigurationStr" %in% names(options)) {
        tmpfilename <- tempfile(pattern = "hdfs_", tmpdir = '/tmp', fileext = ".json")
        f <- file(tmpfilename)
        writeLines(as.character(options["hdfsConfigurationStr"]),f)
        close(f)
        options["hdfsConfigurationFile"] = tmpfilename
    }

    options['fileType'] = 'orc'

    df <- tryCatch({
            ddc_read(url, options)
        }, error=function(e) {
            stop(e)
        }, finally = {
            if(("hdfsConfigurationStr" %in% names(options))) {
                file.remove(tmpfilename)
            }
        })
    df
}

#' Save an R object to a file in HDFS or the local file system.
#'
#' @param object R object to save.
#' @param url File URL. Examples: '/tmp/file.txt', 'hdfs:///file.txt'.
#'
#'                      We also support globbing. Examples: '/tmp/*.orc', 'hdfs:///tmp/*.orc'.
#' @param overwrite Overwrite file if it exists.
#' @param hdfsConfigurationFile By default: \code{paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')}.
#'
#'                              Options are:
#'                              \itemize{
#'                                  \item webhdfsPort: webhdfs port, integer
#'                                  \item hdfsPort: hdfs namenode port, integer
#'                                  \item hdfsHost: hdfs namenode host, string
#'                                  \item hdfsUser: hdfs username, string
#'                              }
#'
#'                              An example file is:
#'
#'                                  \{ \cr
#'                                  "webhdfsPort": 50070, \cr
#'                                  "hdfsPort": 9000, \cr
#'                                  "hdfsHost": "172.17.0.3", \cr
#'                                  "hdfsUser": "jorgem" \cr
#'                                  \}
#' @param hdfsConfigurationStr HDFS configuration in string format. Useful for Distributed R. 
#'                             Normally users specify hdfsConfigurationFile instead of hdfsConfigurationStr.
#' @return Nothing
#' @examples
#' str <- "hello world"; object2hdfs(str, '/tmp/file.txt')

object2hdfs <- function(object, url, overwrite=FALSE,
                        hdfsConfigurationFile=paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep=''),
                        hdfsConfigurationStr='') {
    options = list()
    options['overwrite'] = overwrite
    options['hdfsConfigurationFile'] = hdfsConfigurationFile
    if (hdfsConfigurationStr != '') {
        options['hdfsConfigurationStr'] = hdfsConfigurationStr
    }

#    if(!("hdfsConfigurationFile" %in% names(options))) {
#        # set default hdfsConfigurationFile
#        options["hdfsConfigurationFile"] = paste(system.file(package='hdfsconnector'),'/conf/hdfs.json',sep='')
#    }

    tmpfilename <- ''
    if("hdfsConfigurationStr" %in% names(options)) {
        tmpfilename <- tempfile(pattern = "hdfs_", tmpdir = '/tmp', fileext = ".json")
        f <- file(tmpfilename)
        writeLines(as.character(options["hdfsConfigurationStr"]),f)
        close(f)
        options["hdfsConfigurationFile"] = tmpfilename
    }
    ret <- tryCatch({
            ddc_write(object, url, options)
            TRUE
        }, error=function(e) {
            stop(e)
        }, finally = {
            if(("hdfsConfigurationStr" %in% names(options))) {
                file.remove(tmpfilename)
            }
        })
    ret
}
