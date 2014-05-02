pmr-common
==========
<p>
A common library of MapReduce utilities and code, built against Pivotal HD.  Use at your own risk.

All projects and utilities must be built using Maven.  If you are developing on a system that has PHD installed, you can run the add-mvn-dependencies shell script to add dependencies to your local Maven repository.  The JARs are pulled from the default installation path of Pivotal HD.

See the README under each project for more information.  Current contents includes:

<dl>
  <dt>PivotalMRCommon</dt>
  <dd>A common library for various input formats, utilities, IO types, etc.</dd>
  <dt>HawqIngestTool</dt>
  <dd>A MapReduce analytic to push rows of data from HDFS files to PostgreSQL JDBC driver.  Not intended to be faster than gpfdist or PXF.</dd>
  <dt>HBase Table Migration</dt>
  <dd>This project is an example of a few different classes and command line utilities to migrate an HBase table between two clusters.</dd>
</dl>
