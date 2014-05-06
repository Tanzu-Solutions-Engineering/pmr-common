package com.gopivotal.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.conf.HAWQConfiguration;
import com.pivotal.hawq.mapreduce.metadata.MetadataAccessor;

/**
 * The HawqLoader class wraps the HAWQInputFormat. It converts HAWQRecord
 * objects into Tuples for Pig to process. The HawqLoader will automatically
 * convert the HAWQ table definition to a Pig schema. Details on what data types
 * map to which fields are below. Arrays are not currently supported.<br>
 * <br>
 * First and foremost, <b>users must disable Pig's split combination
 * optimizer</b>. This restriction will be lifted in the future. You do this by
 * adding &quot;SET pig.splitCombination false;&quot; at the top of your Pig
 * script or when you enter the grunt shell. <br>
 * <br>
 * The HawqLoader path is "hawq://<hawq_master>:&lt;port&gt;/&lt;database&gt;".<br>
 * <br>
 * The HawqLoader has three parameters. The table name (required) and then the
 * username and password (optional).<br>
 * <br>
 * An example is below. You can, of course, use command line parameters for any
 * of the options as well as the location string.<br>
 * <br>
 * <pre>
 * SET pig.splitCombination false;
 * A = LOAD 'hawq://mdw1:5432/gpadmin' USING com.gopivotal.pig.HawqLoader('retail_demo.products_dim', 'gpadmin');
 * DESCRIBE A;
 * A: {product_id: int,category_id: int,price: bigdecimal,product_name: chararray}
 * B = LOAD 'hawq://mdw1:5432/gpadmin' USING com.gopivotal.pig.HawqLoader('retail_demo.vinyl_counts', 'gpadmin');
 * DESCRIBE B;
 * B: {category_id: int,count: int}
 * C = JOIN A BY category_id, B by category_id USING 'replicated';
 * STORE C INTO '$output';
 * </pre>
 * 
 * Below is a table describing the mappings from HAWQ to Java to Pig types. <br>
 * <br>
 * <table>
 * <thead align="left">
 * <tr>
 * <th>HAWQ Type</th>
 * <th>Java Type</th>
 * <th>Pig Type</th>
 * </tr>
 * </thead> <tbody>
 * <tr>
 * <td>BIT</td>
 * <td>HAWQVarbit</td>
 * <td>byte</td>
 * </tr>
 * <tr>
 * <td>BOOL</td>
 * <td>boolean</td>
 * <td>boolean</td>
 * </tr>
 * <tr>
 * <td>BOX</td>
 * <td>HAWQBox</td>
 * <td>tuple({pt1: (x: double,y: double),pt2: (x: double,y: double)})</td>
 * </tr>
 * <tr>
 * <td>BPCHAR</td>
 * <td>String</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>BYTEA</td>
 * <td>byte[]</td>
 * <td>bytearray</td>
 * </tr>
 * <tr>
 * <td>CHAR</td>
 * <td>char</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>CIDR</td>
 * <td>HAWQCidr</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>CIRCLE</td>
 * <td>HAWQCircle</td>
 * <td>tuple({x: double,y: double,r: double})</td>
 * </tr>
 * <tr>
 * <td>DATE</td>
 * <td>java.sql.Date</td>
 * <td>datetime (via org.joda.time.DateTime)</td>
 * </tr>
 * <tr>
 * <td>float4</td>
 * <td>float</td>
 * <td>float</td>
 * </tr>
 * <tr>
 * <td>float8</td>
 * <td>double</td>
 * <td>double</td>
 * </tr>
 * <tr>
 * <td>INET</td>
 * <td>HAWQInet</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>INT2</td>
 * <td>short</td>
 * <td>integer</td>
 * </tr>
 * <tr>
 * <td>INT4</td>
 * <td>int</td>
 * <td>integer</td>
 * </tr>
 * <tr>
 * <td>INT8</td>
 * <td>long</td>
 * <td>long</td>
 * </tr>
 * <tr>
 * <td>INTERVAL</td>
 * <td>HAWQInterval</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>LSEG</td>
 * <td>HAWQLseg</td>
 * <td>tuple({pt1: (x: double,y: double),pt2: (x: double,y: double)})</td>
 * </tr>
 * <tr>
 * <td>MACADDR</td>
 * <td>HAWQMacaddr</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>NUMERIC</td>
 * <td>java.math.BigDecimal</td>
 * <td>bigdecimal</td>
 * </tr>
 * <tr>
 * <td>PATH</td>
 * <td>HAWQPath</td>
 * <td>tuple({open: boolean,pts: {x: double,y: double}})</td>
 * </tr>
 * <tr>
 * <td>POINT</td>
 * <td>HAWQPoint</td>
 * <td>tuple({x: double,y: double})</td>
 * </tr>
 * <tr>
 * <td>POLYGON</td>
 * <td>HAWQPolygon</td>
 * <td>tuple({pts: {x: double,y: double},bbox: (pt1: (x: double,y: double),pt2:
 * (x: double,y: double))})</td>
 * </tr>
 * <tr>
 * <td>TEXT</td>
 * <td>String</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>TIME</td>
 * <td>java.sql.Time</td>
 * <td>datetime</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMP</td>
 * <td>java.sql.Timestamp</td>
 * <td>datetime</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMPZ</td>
 * <td>java.sql.Timestamp</td>
 * <td>datetime</td>
 * </tr>
 * <tr>
 * <td>TIMETZ</td>
 * <td>java.sql.Time</td>
 * <td>datetime</td>
 * </tr>
 * <tr>
 * <td>VARBIT</td>
 * <td>HAWQVarbit</td>
 * <td>bytearray</td>
 * </tr>
 * <tr>
 * <td>VARCHAR</td>
 * <td>String</td>
 * <td>chararray</td>
 * </tr>
 * <tr>
 * <td>XML</td>
 * <td>String</td>
 * <td>chararray</td>
 * </tr>
 * </tbody>
 * </table>
 */
public class HawqLoader extends LoadFunc implements LoadMetadata {

	private static final Logger LOG = Logger.getLogger(HawqLoader.class);
	private static final String NUM_FIELDS = "hawq.loader.num.fields";

	private RecordReader<Object, HAWQRecord> rdr = null;
	private ResourceSchema schema = null;

	private TupleFactory tf = TupleFactory.getInstance();

	private String dbUrl = null;
	private String tablename = null;
	private String username = null;
	private String password = null;

	private int fieldCount = -1;

	/**
	 * 
	 * @param tablename
	 *            The table name to use as input
	 * @throws Exception
	 */
	public HawqLoader(String tablename) throws Exception {
		this(tablename, null, null);
	}

	public HawqLoader(String tablename, String username) throws Exception {
		this(tablename, username, null);
	}

	public HawqLoader(String tablename, String username, String password)
			throws Exception {
		this.tablename = tablename;
		this.username = username;
		this.password = password;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new HAWQInputFormat();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToRead(RecordReader rdr, PigSplit split)
			throws IOException {
		// Save the reader
		this.rdr = (RecordReader<Object, HAWQRecord>) rdr;
		// Get the number of fields from our set parameter
		fieldCount = Integer.parseInt(UDFContext.getUDFContext()
				.getClientSystemProps().getProperty(NUM_FIELDS));
	}

	@Override
	public Tuple getNext() throws IOException {
		synchronized (this) {
			try {
				if (rdr.nextKeyValue()) {
					// Create the new tuple
					Tuple retval = tf.newTuple(fieldCount);

					HAWQRecord record = rdr.getCurrentValue();

					// start at 1...
					for (int i = 1; i <= fieldCount; ++i) {
						// Set the field of the tuple to the converted pig value
						retval.set(i - 1,
								HawqPigDataConverter.toPigValue(record, i));
					}

					return retval;
				} else {
					return null;
				}
			} catch (InterruptedException e) {
				LOG.error("Caught error during getNext()", e);
				throw new IOException(e);
			}
		}
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		this.dbUrl = location.replaceAll("hawq://", "");

		try {
			// Set job parameters here using configuration
			HAWQInputFormat.setInput(job.getConfiguration(), dbUrl, username,
					password, tablename);

			// Set the number of fields from the schema so we can get it on the
			// backend
			Properties props = new Properties();
			props.setProperty(
					NUM_FIELDS,
					Integer.toString(HAWQConfiguration.getInputTableSchema(
							job.getConfiguration()).getFieldCount()));

			UDFContext.getUDFContext().setClientSystemProps(props);

		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		// Get schema if it does not already exist
		if (schema == null) {
			// Remove the HAWQ scheme
			this.dbUrl = location.replaceAll("hawq://", "");
			try {

				// Get the metadata schema and run it through the converter
				MetadataAccessor accessor = MetadataAccessor
						.newInstanceUsingJDBC(dbUrl, username, password,
								tablename);

				switch (accessor.getTableFormat()) {
				case AO:
					schema = HawqPigDataConverter.toPigSchema(accessor
							.getAOMetadata().getSchema());
					break;
				default:
					throw new IOException("Only AO tables are supported");
				}
			} catch (Exception e) {
				throw new IOException("Failed to convert schema", e);
			}
		}

		return schema;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression arg0) throws IOException {

	}
}
