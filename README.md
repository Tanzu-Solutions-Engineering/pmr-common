pmr-common
==========

A common library of MapReduce utilities and code.

Current contents includes:

<dl>
  <dt>PivotalMRCommon</dt>
  <dd>A common library for various input formats, utilities, IO types, etc.</dd>
  <dt>HawqIngestTool</dt>
  <dd>A MapReduce analytic to push rows of data from HDFS files to PostgreSQL JDBC driver.  Not intended to be faster than gpfdist or PXF.</dd>
  <dt>HawqErrorCheck</dt>
  <dd>A MapReduce analytic intended to be a workaround for the current "error table" issue.  Validates the number of columns in a row and the data type, generating "good" and "bad" data sets.</dd>
</dl>
