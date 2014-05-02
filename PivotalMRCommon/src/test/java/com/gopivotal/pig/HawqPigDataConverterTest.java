package com.gopivotal.pig;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.ao.io.HAWQAORecord;
import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQCidr;
import com.pivotal.hawq.mapreduce.datatype.HAWQCircle;
import com.pivotal.hawq.mapreduce.datatype.HAWQInet;
import com.pivotal.hawq.mapreduce.datatype.HAWQInterval;
import com.pivotal.hawq.mapreduce.datatype.HAWQLseg;
import com.pivotal.hawq.mapreduce.datatype.HAWQMacaddr;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import com.pivotal.hawq.mapreduce.datatype.HAWQVarbit;
import com.pivotal.hawq.mapreduce.datatype.HAWQInet.InetType;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType;

public class HawqPigDataConverterTest {

	private BagFactory bf = BagFactory.getInstance();
	private TupleFactory tf = TupleFactory.getInstance();

	@Before
	public void setup() throws FileNotFoundException {

	}

	@Test
	public void testBitSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BIT, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testBitValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BIT, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setBit(1, new HAWQVarbit("1001001"));

		Assert.assertEquals("1001001",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testBoolSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BOOL, false);

		Assert.assertEquals("field: boolean",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testBoolValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BOOL, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setBoolean(1, true);

		Assert.assertEquals(true, HawqPigDataConverter.toPigValue(record, 1));

		record.setBoolean(1, false);

		Assert.assertEquals(false, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testBoxSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BOX, false);

		Assert.assertEquals(
				"field: tuple({pt1: (x: double,y: double),pt2: (x: double,y: double)})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testBoxValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BOX, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setBox(1, new HAWQBox(0.0, 1.0, 1.3, 1.8));

		Tuple t = tf.newTuple(2);
		Tuple pt1 = tf.newTuple(2);
		Tuple pt2 = tf.newTuple(2);

		pt1.set(0, 0.0);
		pt1.set(1, 1.0);
		pt2.set(0, 1.3);
		pt2.set(1, 1.8);

		t.set(0, pt1);
		t.set(1, pt2);

		Assert.assertEquals(t, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testBpCharSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BPCHAR, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testBpCharValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BPCHAR, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setString(1, "somechars");

		Assert.assertEquals("somechars",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testByteArraySchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BYTEA, false);

		Assert.assertEquals("field: bytearray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testByteArrayValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.BYTEA, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setBytes(1, "somebytes".getBytes());

		DataByteArray bytes = (DataByteArray) HawqPigDataConverter.toPigValue(
				record, 1);
		Assert.assertTrue(new DataByteArray("somebytes".getBytes())
				.equals(bytes));
	}

	@Test
	public void testCharSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CHAR, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testCharValueConversion() throws Exception {

		// This CHAR turns it to a BPCHAR in the HAWQPrimitiveField constructor,
		// and a BPChAR is a string
		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CHAR, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setChar(1, 'a');

		Assert.assertEquals("a", HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testCidrSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CIDR, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testCidrValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CIDR, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		byte[] ipv4_1 = { 0x7F, 0x0, 0x0, 0x1 };
		record.setCidr(1, new HAWQCidr(InetType.IPV4, ipv4_1, (short) 32));

		Assert.assertEquals("127.0.0.1/32",
				HawqPigDataConverter.toPigValue(record, 1));

		byte[] ipv4_2 = { (byte) 0xC0, (byte) 0xA8, 0x0, 0x08 };
		record.setCidr(1, new HAWQCidr(InetType.IPV4, ipv4_2, (short) 28));

		Assert.assertEquals("192.168.0.8/28",
				HawqPigDataConverter.toPigValue(record, 1));

		byte[] ipv6_1 = { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };
		record.setCidr(1, new HAWQCidr(InetType.IPV6, ipv6_1, (short) 32));
		Assert.assertEquals("::0/32",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testCircleSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CIRCLE, false);

		Assert.assertEquals("field: tuple({x: double,y: double,r: double})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testCircleValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.CIRCLE, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setCircle(1, new HAWQCircle(0.0, 1.0, 2.0));

		Tuple t = tf.newTuple(3);

		t.set(0, 0.0);
		t.set(1, 1.0);
		t.set(2, 2.0);

		Assert.assertEquals(t, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testDateSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.DATE, false);

		Assert.assertEquals("field: datetime",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testDateValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.DATE, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setDate(1, new Date(1397088000000L));

		Assert.assertEquals(new DateTime(1397088000000L),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testFloat4SchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.FLOAT4, false);

		Assert.assertEquals("field: float",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testFloat4ValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.FLOAT4, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setFloat(1, 3.14f);

		Assert.assertEquals(3.14f, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testFloat8SchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.FLOAT8, false);

		Assert.assertEquals("field: double",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testFloat8ValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.FLOAT8, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setDouble(1, 3.14159265359);

		Assert.assertEquals(3.14159265359,
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testInetSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INET, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testInetValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INET, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setInet(1, new HAWQInet("127.0.0.1"));

		Assert.assertEquals("127.0.0.1",
				HawqPigDataConverter.toPigValue(record, 1));

		record.setInet(1, new HAWQInet("127.0.0.1/24"));

		Assert.assertEquals("127.0.0.1/24",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testInt2SchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT2, false);

		Assert.assertEquals("field: int",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testInt2ValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT2, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setShort(1, (short) 56);

		Assert.assertEquals((short) 56,
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testInt4SchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT4, false);

		Assert.assertEquals("field: int",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testInt4ValueConversion() throws Exception {

		// This CHAR turns it to a BPCHAR in the HAWQPrimitiveField constructor,
		// and a BPChAR is a string
		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT4, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setInt(1, 4);

		Assert.assertEquals(4, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testInt8SchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT8, false);

		Assert.assertEquals("field: long",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testInt8ValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INT8, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setLong(1, 256L);

		Assert.assertEquals(256L, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testIntervalSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INTERVAL, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testIntervalValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.INTERVAL, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setInterval(1, new HAWQInterval(2014, 04, 10, 12, 12, 12));

		Assert.assertEquals("2014 years 4 mons 10 days 12:12:12",
				HawqPigDataConverter.toPigValue(record, 1));
		record.setInterval(1, new HAWQInterval(2014, 04, 10, 12, 12, 12, 56,
				908));

		Assert.assertEquals("2014 years 4 mons 10 days 12:12:12.056908",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testLsegSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.LSEG, false);

		Assert.assertEquals(
				"field: tuple({pt1: (x: double,y: double),pt2: (x: double,y: double)})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testLsegValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.LSEG, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setLseg(1, new HAWQLseg(0.0, 1.0, 2.0, 3.0));

		Tuple t = tf.newTuple(2);
		Tuple pt1 = tf.newTuple(2);
		Tuple pt2 = tf.newTuple(2);

		pt1.set(0, 0.0);
		pt1.set(1, 1.0);
		pt2.set(0, 2.0);
		pt2.set(1, 3.0);

		t.set(0, pt1);
		t.set(1, pt2);

		Assert.assertEquals(t, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testMacAddrSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.MACADDR, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testMacAddrValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.MACADDR, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setMacaddr(1, new HAWQMacaddr(new byte[] { 0x0, 0x1, 0x2, 0x3,
				0x4, 0x5 }));

		Assert.assertEquals("00:01:02:03:04:05",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testNumericSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.NUMERIC, false);

		Assert.assertEquals("field: bigdecimal", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testNumericValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.NUMERIC, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setBigDecimal(1, new BigDecimal(3.1415926));

		Assert.assertEquals(new BigDecimal(3.1415926),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testPathSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.PATH, false);

		Assert.assertEquals(
				"field: tuple({open: boolean,pts: {x: double,y: double}})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testPathValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.PATH, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setPath(1, new HAWQPath(true, new HAWQPoint(0.0, 1.0),
				new HAWQPoint(1.0, 2.0), new HAWQPoint(2.0, 3.0)));

		Tuple t = tf.newTuple(2);
		Tuple pt1 = tf.newTuple(2);
		Tuple pt2 = tf.newTuple(2);
		Tuple pt3 = tf.newTuple(2);

		pt1.set(0, 0.0);
		pt1.set(1, 1.0);
		pt2.set(0, 1.0);
		pt2.set(1, 2.0);
		pt3.set(0, 2.0);
		pt3.set(1, 3.0);

		List<Tuple> tList = new ArrayList<Tuple>();

		tList.add(pt1);
		tList.add(pt2);
		tList.add(pt3);

		t.set(0, true);
		t.set(1, bf.newDefaultBag(tList));

		Assert.assertEquals(t, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testPointSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.POINT, false);

		Assert.assertEquals("field: tuple({x: double,y: double})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testPointValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.POINT, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setPoint(1, new HAWQPoint(0.0, 1.0));

		Tuple pt1 = tf.newTuple(2);

		pt1.set(0, 0.0);
		pt1.set(1, 1.0);

		Assert.assertEquals(pt1, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testPolygonSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.POLYGON, false);

		Assert.assertEquals(
				"field: tuple({pts: {x: double,y: double},bbox: (pt1: (x: double,y: double),pt2: (x: double,y: double))})",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testPolygonValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.POLYGON, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		List<HAWQPoint> points = new ArrayList<HAWQPoint>();
		points.add(new HAWQPoint(0.0, 0.0));
		points.add(new HAWQPoint(1.0, 1.0));
		points.add(new HAWQPoint(2.0, 2.0));

		record.setPolygon(1, new HAWQPolygon(points, new HAWQBox(0.0, 0.0, 2.0,
				2.0)));

		Tuple t = tf.newTuple(2);
		Tuple pt1 = tf.newTuple(2);
		Tuple pt2 = tf.newTuple(2);
		Tuple pt3 = tf.newTuple(2);

		pt1.set(0, 0.0);
		pt1.set(1, 0.0);
		pt2.set(0, 1.0);
		pt2.set(1, 1.0);
		pt3.set(0, 2.0);
		pt3.set(1, 2.0);

		List<Tuple> tList = new ArrayList<Tuple>();

		tList.add(pt1);
		tList.add(pt2);
		tList.add(pt3);

		Tuple bbox = tf.newTuple(2);
		Tuple bboxPt1 = tf.newTuple(2);
		Tuple bboxPt2 = tf.newTuple(2);

		bboxPt1.set(0, 0.0);
		bboxPt1.set(1, 0.0);
		bboxPt2.set(0, 2.0);
		bboxPt2.set(1, 2.0);
		bbox.set(0, bboxPt1);
		bbox.set(1, bboxPt2);

		t.set(0, bf.newDefaultBag(tList));
		t.set(1, bbox);

		Assert.assertEquals(t, HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testTextSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TEXT, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testTextValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TEXT, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setString(1, "somedata");

		Assert.assertEquals("somedata",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testTimeSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIME, false);

		Assert.assertEquals("field: datetime",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testTimeValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIME, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setTime(1, new Time(1397088000000L));

		Assert.assertEquals(new DateTime(1397088000000L),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testTimeStampSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMESTAMP, false);

		Assert.assertEquals("field: datetime",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testTimeStampValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMESTAMP, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setTimestamp(1, new Timestamp(1397088000000L));

		Assert.assertEquals(new DateTime(1397088000000L),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testTimeStampTzSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMESTAMPTZ, false);

		Assert.assertEquals("field: datetime",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testTimeStampTzValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMESTAMPTZ, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setTimestamp(1, new Timestamp(1397088000000L));

		Assert.assertEquals(new DateTime(1397088000000L),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testTimeTzSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMETZ, false);

		Assert.assertEquals("field: datetime",
				HawqPigDataConverter.toPigField(hField).toString());
	}

	@Test
	public void testTimeTzValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.TIMETZ, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setTime(1, new Time(1397088000000L));

		Assert.assertEquals(new DateTime(1397088000000L),
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testVarBitSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.VARBIT, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testVarBitValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.VARBIT, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setVarbit(1, new HAWQVarbit("001010010"));

		Assert.assertEquals("001010010",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testVarCharSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.VARCHAR, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testVarCharValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.VARCHAR, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setString(1, "somedata");

		Assert.assertEquals("somedata",
				HawqPigDataConverter.toPigValue(record, 1));
	}

	@Test
	public void testXmlSchemaConversion() throws FrontendException {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.XML, false);

		Assert.assertEquals("field: chararray", HawqPigDataConverter
				.toPigField(hField).toString());
	}

	@Test
	public void testXmlValueConversion() throws Exception {

		HAWQPrimitiveField hField = new HAWQPrimitiveField(false, "field",
				PrimitiveType.XML, false);

		HAWQSchema schema = new HAWQSchema("schema", hField);
		HAWQRecord record = new HAWQAORecord(schema, "UTF-8", "version");

		record.setString(1,
				"<property><name>isXmlGood</name><value>No</value></property>");

		Assert.assertEquals(
				"<property><name>isXmlGood</name><value>No</value></property>",
				HawqPigDataConverter.toPigValue(record, 1));
	}
}
