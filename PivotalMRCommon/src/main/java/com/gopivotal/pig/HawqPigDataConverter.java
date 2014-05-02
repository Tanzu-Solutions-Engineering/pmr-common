package com.gopivotal.pig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.joda.time.DateTime;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQCircle;
import com.pivotal.hawq.mapreduce.datatype.HAWQLseg;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.schema.HAWQField;

/**
 * A utility for converting HAWQ column definitions and data to their related
 * counterparts for Pig.
 */
public class HawqPigDataConverter {

	private static final Logger LOG = Logger
			.getLogger(HawqPigDataConverter.class);
	private static final Map<PrimitiveType, Byte> hawqToPigType = new HashMap<PrimitiveType, Byte>();
	private static TupleFactory TF = TupleFactory.getInstance();
	private static BagFactory BF = BagFactory.getInstance();

	public static FieldSchema toPigField(HAWQField hField)
			throws FrontendException {

		// Create a few objects that are re-used in the below switch statement
		List<FieldSchema> ptFields = new ArrayList<FieldSchema>();
		ptFields.add(new FieldSchema("x", DataType.DOUBLE));
		ptFields.add(new FieldSchema("y", DataType.DOUBLE));

		List<FieldSchema> boxFields = new ArrayList<FieldSchema>();
		boxFields.add(new FieldSchema("pt1", new Schema(ptFields)));
		boxFields.add(new FieldSchema("pt2", new Schema(ptFields)));

		// Convert the HAWQ field to the pig data type
		byte pigType = getPigDataType(hField.asPrimitive().getType());

		// Based on the HAWQ type, create the associated schema. Complex types
		// are commonly tuples containing other fields. Simple types are
		// trivially converted.
		FieldSchema fSchema = null;
		switch (hField.asPrimitive().getType()) {
		case BOX:
			fSchema = new FieldSchema(hField.getName(), new Schema(boxFields));
			break;
		case CIRCLE:
			List<FieldSchema> circleFields = new ArrayList<FieldSchema>();

			circleFields.add(new FieldSchema("x", DataType.DOUBLE));
			circleFields.add(new FieldSchema("y", DataType.DOUBLE));
			circleFields.add(new FieldSchema("r", DataType.DOUBLE));

			fSchema = new FieldSchema(hField.getName(),
					new Schema(circleFields));
			break;
		case LSEG:
			List<FieldSchema> lsegFields = new ArrayList<FieldSchema>();

			lsegFields.add(new FieldSchema("pt1", new Schema(ptFields)));
			lsegFields.add(new FieldSchema("pt2", new Schema(ptFields)));

			fSchema = new FieldSchema(hField.getName(), new Schema(lsegFields));
			break;
		case PATH:
			List<FieldSchema> pathFields = new ArrayList<FieldSchema>();

			pathFields.add(new FieldSchema("open", DataType.BOOLEAN));
			pathFields.add(new FieldSchema("pts", new Schema(ptFields),
					DataType.BAG));

			fSchema = new FieldSchema(hField.getName(), new Schema(pathFields));
			break;
		case POINT:
			fSchema = new FieldSchema(hField.getName(), new Schema(ptFields));
			break;
		case POLYGON:
			List<FieldSchema> polygonFields = new ArrayList<FieldSchema>();
			polygonFields.add(new FieldSchema("pts", new Schema(ptFields),
					DataType.BAG));

			polygonFields.add(new FieldSchema("bbox", new Schema(boxFields)));

			fSchema = new FieldSchema(hField.getName(), new Schema(
					polygonFields));
			break;
		default:
			// All the types that are not complex, i.e. the ones above
			fSchema = new FieldSchema(hField.getName(), pigType);
			break;
		}

		return fSchema;
	}

	/**
	 * Convert a HAWQSchema to a ResourceSchema.
	 * 
	 * @param schema
	 * @return The Pig schema or null if an exception was thrown
	 */
	public static synchronized ResourceSchema toPigSchema(HAWQSchema schema) {

		List<HAWQField> hawqFields = schema.getFields();
		List<FieldSchema> pigFields = new ArrayList<FieldSchema>();

		try {
			// For each field, convert it to a big type
			for (HAWQField hField : hawqFields) {
				if (hField.isPrimitive()) {
					pigFields.add(toPigField(hField));
				} else if (hField.isArray()) {
					throw new UnsupportedOperationException(
							"Array types are not supported");
				}
			}
		} catch (FrontendException e) {
			LOG.error("Error processing Pig schema", e);
			return null;
		}

		return new ResourceSchema(new Schema(pigFields));
	}

	/**
	 * Converts the field, retrieved from the given record at the given index,
	 * into a Pig value, expressed as an object. Inspects the field's type for
	 * proper conversion
	 * 
	 * @param record
	 *            The record to get the field from
	 * @param idx
	 *            The index of the field
	 * @return The Pig object or null on error
	 */
	public static synchronized Object toPigValue(HAWQRecord record, int idx) {
		Object retval = null;
		PrimitiveType hawqType = record.getSchema().getField(idx).asPrimitive()
				.getType();
		try {
			switch (hawqType) {
			case BIT:
				return record.getBit(idx).toString();
			case BOOL:
				return record.getBoolean(idx);
			case BOX:
				HAWQBox box = record.getBox(idx);
				HAWQPoint pt1 = box.getPoint1();
				HAWQPoint pt2 = box.getPoint2();

				Tuple pigBox = TF.newTuple(2);
				Tuple tPt1 = TF.newTuple(2);
				Tuple tPt2 = TF.newTuple(2);

				tPt1.set(0, pt1.getX());
				tPt1.set(1, pt1.getY());
				tPt2.set(0, pt2.getX());
				tPt2.set(1, pt2.getY());

				pigBox.set(0, tPt1);
				pigBox.set(1, tPt2);

				return pigBox;
			case BPCHAR:
				return record.getString(idx);
			case BYTEA:
				return new DataByteArray(record.getBytes(idx));
			case CHAR:
				// this case will never get called cause CHAR -> BPCHAR
				return (char) record.getChar(idx);
			case CIDR:
				return record.getCidr(idx).toString();
			case CIRCLE:
				HAWQCircle cirle = record.getCircle(idx);
				Tuple tCircle = TF.newTuple(3);

				tCircle.set(0, cirle.getCenter().getX());
				tCircle.set(1, cirle.getCenter().getY());
				tCircle.set(2, cirle.getRadius());

				return tCircle;
			case DATE:
				return new DateTime(record.getDate(idx).getTime());
			case FLOAT4:
				return record.getFloat(idx);
			case FLOAT8:
				return record.getDouble(idx);
			case INET:
				return record.getInet(idx).toString();
			case INT2:
				return record.getShort(idx);
			case INT4:
				return record.getInt(idx);
			case INT8:
				return record.getLong(idx);
			case INTERVAL:
				return record.getInterval(idx).toString();
			case LSEG:
				HAWQLseg lseg = record.getLseg(idx);
				pt1 = lseg.getPoint1();
				pt2 = lseg.getPoint2();

				Tuple tLSeg = TF.newTuple(2);
				tPt1 = TF.newTuple(2);
				tPt2 = TF.newTuple(2);

				tPt1.set(0, pt1.getX());
				tPt1.set(1, pt1.getY());
				tPt2.set(0, pt2.getX());
				tPt2.set(1, pt2.getY());

				tLSeg.set(0, tPt1);
				tLSeg.set(1, tPt2);
				return tLSeg;
			case MACADDR:
				return record.getMacaddr(idx).toString();
			case NUMERIC:
				return record.getBigDecimal(idx);
			case PATH:
				HAWQPath path = record.getPath(idx);
				List<Tuple> points = new ArrayList<Tuple>();

				for (HAWQPoint p : path.getPoints()) {
					Tuple tPoint = TF.newTuple(2);
					tPoint.set(0, p.getX());
					tPoint.set(1, p.getY());
					points.add(tPoint);
				}

				Tuple tPath = TF.newTuple(2);

				tPath.set(0, path.isOpen());
				tPath.set(1, BF.newDefaultBag(points));

				return tPath;
			case POINT:
				HAWQPoint pt = record.getPoint(idx);
				Tuple tPoint = TF.newTuple(2);
				tPoint.set(0, pt.getX());
				tPoint.set(1, pt.getY());
				return tPoint;
			case POLYGON:
				Tuple tPoly = TF.newTuple(2);

				HAWQPolygon poly = record.getPolygon(idx);
				points = new ArrayList<Tuple>();

				for (HAWQPoint p : poly.getPoints()) {
					tPoint = TF.newTuple(2);
					tPoint.set(0, p.getX());
					tPoint.set(1, p.getY());
					points.add(tPoint);
				}

				box = poly.getBoundbox();
				pt1 = box.getPoint1();
				pt2 = box.getPoint2();

				pigBox = TF.newTuple(2);
				tPt1 = TF.newTuple(2);
				tPt2 = TF.newTuple(2);

				tPt1.set(0, pt1.getX());
				tPt1.set(1, pt1.getY());
				tPt2.set(0, pt2.getX());
				tPt2.set(1, pt2.getY());

				pigBox.set(0, tPt1);
				pigBox.set(1, tPt2);

				tPoly.set(0, BF.newDefaultBag(points));
				tPoly.set(1, pigBox);

				return tPoly;
			case TEXT:
				return record.getString(idx);
			case TIME:
			case TIMETZ:
				return new DateTime(record.getTime(idx).getTime());
			case TIMESTAMP:
			case TIMESTAMPTZ:
				return new DateTime(record.getTimestamp(idx).getTime());
			case VARBIT:
				return record.getVarbit(idx).toString();
			case VARCHAR:
				return record.getString(idx);
			case XML:
				return record.getString(idx);
			default:
				break;

			}
		} catch (HAWQException | ExecException e) {
			LOG.error("Error when converting object", e);
			return null;
		}

		return retval;
	}

	/**
	 * Gets the Pig data type from the primitive HAWQ type. O(1)
	 * 
	 * @param hawqType
	 *            The HAWQ Type
	 * @return The Pig type or null if not found in the internal map
	 */
	public static synchronized byte getPigDataType(PrimitiveType hawqType) {
		return hawqToPigType.get(hawqType);
	}

	/**
	 * Gets the HAWQ data type from from the given Pig {@link DataType}. O(n)
	 * 
	 * @param pigType
	 *            The Pig type
	 * @return The HAWQ type or null if not found
	 */
	public static synchronized PrimitiveType getHawqDataType(byte pigType) {

		for (Entry<PrimitiveType, Byte> entry : hawqToPigType.entrySet()) {
			if (entry.getValue() == pigType) {
				return entry.getKey();
			}
		}

		return null;
	}

	static {
		// A bunch of mappings that I selfishly chose
		hawqToPigType.put(PrimitiveType.BIT, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.BOOL, DataType.BOOLEAN);
		hawqToPigType.put(PrimitiveType.BOX, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.BPCHAR, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.BYTEA, DataType.BYTEARRAY);
		hawqToPigType.put(PrimitiveType.CHAR, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.CIDR, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.CIRCLE, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.DATE, DataType.DATETIME);
		hawqToPigType.put(PrimitiveType.FLOAT4, DataType.FLOAT);
		hawqToPigType.put(PrimitiveType.FLOAT8, DataType.DOUBLE);
		hawqToPigType.put(PrimitiveType.INET, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.INT2, DataType.INTEGER);
		hawqToPigType.put(PrimitiveType.INT4, DataType.INTEGER);
		hawqToPigType.put(PrimitiveType.INT8, DataType.LONG);
		hawqToPigType.put(PrimitiveType.INTERVAL, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.LSEG, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.MACADDR, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.NUMERIC, DataType.BIGDECIMAL);
		hawqToPigType.put(PrimitiveType.PATH, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.POINT, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.POLYGON, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.TEXT, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.TIME, DataType.DATETIME);
		hawqToPigType.put(PrimitiveType.TIMESTAMP, DataType.DATETIME);
		hawqToPigType.put(PrimitiveType.TIMESTAMPTZ, DataType.DATETIME);
		hawqToPigType.put(PrimitiveType.TIMETZ, DataType.DATETIME);
		hawqToPigType.put(PrimitiveType.VARBIT, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.VARCHAR, DataType.CHARARRAY);
		hawqToPigType.put(PrimitiveType.XML, DataType.CHARARRAY);
	}
}
