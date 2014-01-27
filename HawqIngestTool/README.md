HAWQ Ingest Tool
==========
<p>
This utility accepts a delimited data set and uses MapReduce and a JDBC connection to write to a HAWQ table in parallel.  The tool can be extended to write custom mapper implementations versus a delimited flat file.

The default delimiter is a pipe '|', but can be set to anything via the command line.

```sh
hadoop jar hawq-ingest-tool-1.0.0.jar com.gopivotal.hawq.mapreduce.HawqIngestTool --help

usage: hadoop jar <jarfile> [opts]
 -d,--database <arg>      HAWQ database
    --delimiter <arg>     Delimiter of the rows of data.  Default is pipe - |
 -h,--host <arg>          HAWQ hostname
    --help                Print this help message
 -i,--input <arg>         CSV list of input
 -m,--nummappers <arg>    Specifying this parameter will combine blocks into a set number of map tasks
 -n,--numreducers <arg>   Number of reducers.  Default is based on cluster configuration
 -o,--output <arg>        Output directory
 -p,--port <arg>          HAWQ port
 -s,--password <arg>      HAWQ password
 -t,--table <arg>         HAWQ table
 -u,--user <arg>          HAWQ username
```