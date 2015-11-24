package brickhouse.udf.xunit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;



public class XUnitTest extends TestCase {
	
	// The struct defining segmentation dimensions always has three fields.
	static String[] fieldNames = {"dim", "attr_names", "attr_values"};

	/**
	 * We can use the same OI for all instances of a seg dimension struct.
	 */
	StandardStructObjectInspector getStructOI() {
		ArrayList<String> fieldNameList = new ArrayList<String>(Arrays.asList(fieldNames));
		ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ArrayList<ObjectInspector> oiList = new ArrayList<ObjectInspector>(3);
		oiList.add(stringOI);
		oiList.add(ObjectInspectorFactory.getStandardListObjectInspector(stringOI));
		oiList.add(ObjectInspectorFactory.getStandardListObjectInspector(stringOI));
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNameList, oiList);
	}

	/**
	 * A convenience method for creating a seg dimension struct object instance.
	 */
	ArrayList<Object> getStructObject(String dim, List<String> attrNames, List<String> attrVals) {
		ArrayList<Object> struct = new ArrayList<Object>(3);
		struct.add(dim);
		struct.add(attrNames);
		struct.add(attrVals);
		return struct;
	}

	/**
	 * A convenience method for validating a seg dimension struct object instance.
	 */
	void validateStructObject(Object struct, String dim, List<String> attrNames, List<String> attrVals) {
		StandardStructObjectInspector soi = getStructOI();
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		assertEquals(dim, soi.getStructFieldData(struct, fields.get(0)));
		assertEquals(attrNames, soi.getStructFieldData(struct, fields.get(1)));
		assertEquals(attrVals, soi.getStructFieldData(struct, fields.get(2)));
	}
	
	@Test
	public void testInitialize() throws UDFArgumentException {
		GenericUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		// check we can initialize using the structOI without throwing an exception
		xploder.initialize(oiList);
	}

	@Test
	public void testProcessOneOne() throws UDFArgumentException, HiveException {
		GenericUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		xploder.initialize(oiList);
		
		List<String> nList = new ArrayList<String>(1);
		nList.add(0, "alpha");
		List<String> vList = new ArrayList<String>(1);
		vList.add(0, "a");
		
		Object struct = getStructObject("adim", nList, vList);
		validateStructObject(struct, "adim", nList, vList);
		
		ArrayList<Object> structList = new ArrayList<Object>(1);
		structList.add(struct);
		Object[] dims = { structList };
		xploder.process(dims);
	}
	
	/**
	 * Generates an instance of a seg dim struct with two attribute levels.
	 */
	Object getOrdTwoStruct(String ordVal, String subordVal) {
		List<String> attrNameList = new ArrayList<String>(2);
		attrNameList.add(0, "ord");
		attrNameList.add(1, "subord");
		List<String> attrValueList = new ArrayList<String>(2);
		attrValueList.add(0, ordVal);
		attrValueList.add(1, subordVal);
		Object struct = getStructObject("odim", attrNameList, attrValueList);
		validateStructObject(struct, "odim", attrNameList, attrValueList);
		return struct;
	}
	
	/**
	 * Generates an instance of a seg dim struct with three attribute levels.
	 */
	Object getAlphaThreeStruct(String alphaVal, String betaVal, String gammaVal) {
		List<String> attrNameList = new ArrayList<String>(3);
		attrNameList.add(0, "alpha");
		attrNameList.add(1, "beta");
		attrNameList.add(2, "gamma");
		List<String> attrValueList = new ArrayList<String>(3);
		attrValueList.add(0, alphaVal);
		attrValueList.add(1, betaVal);
		attrValueList.add(2, gammaVal);
		Object struct = getStructObject("adim", attrNameList, attrValueList);
		validateStructObject(struct, "adim", attrNameList, attrValueList);
		return struct;
	}
	
	@Test
	public void testProcessOrd() throws UDFArgumentException, HiveException {
		GenericUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		xploder.initialize(oiList);

		ArrayList<Object> structList = new ArrayList<Object>(1);
		structList.add(getOrdTwoStruct("1", "2"));
		Object[] dims = { structList };
		xploder.process(dims);
	}

	
	@Test
	public void testProcessOrdAlpha() throws UDFArgumentException, HiveException {
		GenericUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		xploder.initialize(oiList);

		ArrayList<Object> structList = new ArrayList<Object>(2);
		structList.add(getOrdTwoStruct("1", "2"));
		structList.add(getAlphaThreeStruct("a", "b", "c"));
		Object[] dims = { structList };
		xploder.process(dims);
	}

	/**
	 * Generates an instance of an event seg dim struct
	 */
	Object getEventStruct(String eventVal) {
		List<String> attrNameList = new ArrayList<String>(1);
		attrNameList.add(0, "e");
		List<String> attrValueList = new ArrayList<String>(1);
		attrValueList.add(0, eventVal);
		Object struct = getStructObject("event", attrNameList, attrValueList);
		validateStructObject(struct, "event", attrNameList, attrValueList);
		return struct;
	}
	
	/**
	 * Generates an instance of a spam seg dim struct
	 */
	Object getSpamStruct(String spamVal) {
		List<String> attrNameList = new ArrayList<String>(1);
		attrNameList.add(0, "is_spam");
		List<String> attrValueList = new ArrayList<String>(1);
		attrValueList.add(0, spamVal);
		Object struct = getStructObject("spam", attrNameList, attrValueList);
		validateStructObject(struct, "spam", attrNameList, attrValueList);
		return struct;
	}
	
	@Test
	public void testProcessFourDim() throws UDFArgumentException, HiveException {
		GenericUDTF xploder = new XUnitExplodeUDTF();
		ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
		xploder.initialize(oiList);

		ArrayList<Object> structList = new ArrayList<Object>(4);
		structList.add(getOrdTwoStruct("1", "2"));
		structList.add(getAlphaThreeStruct("a", "b", "c"));
		structList.add(getEventStruct("s_page_view_api"));
		structList.add(getEventStruct("nonspammer-validated"));
		Object[] dims = { structList };
		xploder.process(dims);
	}
	
}