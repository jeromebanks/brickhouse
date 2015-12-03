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
		//xploder.process(dims);
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
		//xploder.process(dims);
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
		//xploder.process(dims);
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
		//xploder.process(dims);
	}

    @Test
    public void testEventExplodeXUnit() throws UDFArgumentException, HiveException {
        GenericUDTF xploder = new XUnitExplodeUDTF();
        ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        PrimitiveObjectInspectorFactory.javaIntObjectInspector.set(intOI,4);
        ObjectInspector boolOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.set(boolOI,true);
        ObjectInspector[] oi = new ObjectInspector[3];
        //oi[0] = ObjectoiList;
        xploder.initialize(oiList);

        ArrayList<Object> structList = new ArrayList<Object>();

        List<String> nList0 = new ArrayList<String>();
        nList0.add("e");
        List<String> vList0 = new ArrayList<String> ();
        vList0.add("meetme");
        Object struct0 = getStructObject("event",nList0,vList0);
        structList.add(struct0);

        List<String> nList5 = new ArrayList<String>();
        nList5.add("is_spam");
        List<String> vList5 = new ArrayList<String> ();
        vList5.add("nonspammer-validated");
        Object struct5 = getStructObject("spam",nList5,vList5);
        structList.add(struct5);

        List<String> nList2 = new ArrayList<String>();
        nList2.add("c1_profile_view__friends");
        //nList2.add("null");
        List<String> vList2 = new ArrayList<String>();
        vList2.add("NA");
        Object struct2 = getStructObject("custom",nList2,vList2);
        structList.add(struct2);

        List<String> nList7 = new ArrayList<String>();
        nList7.add("c2_profile_view__platform");
        //nList7.add("null");
        List<String> vList7 = new ArrayList<String>();
        vList7.add("Web");
        Object struct7 = getStructObject("custom",nList7,vList7);
        structList.add(struct7);

        List<String> nList8 = new ArrayList<String>();
        nList8.add("test");
        List<String> vList8 = new ArrayList<String>();
        vList8.add("W2");
        Object struct8 = getStructObject("custom",nList8,vList8);
        structList.add(struct8);


        List<String> nList = new ArrayList<String>(1);
        nList.add(0, "bucket");
        List<String> vList = new ArrayList<String>(1);
        vList.add(0, "25-34");
        Object struct = getStructObject("age", nList, vList);
        //validateStructObject(struct, "age", nList, vList);
        structList.add(struct);



        List<String> nList4 = new ArrayList<String>();
        nList4.add("continent");nList4.add("country");nList4.add("state");
        List<String> vList4 = new ArrayList<String> ();
        vList4.add("NA");vList4.add("USA");vList4.add("CA");
        Object struct4 = getStructObject("geo",nList4,vList4);
        structList.add(struct4);

        List<String> nList3 = new ArrayList<String>();
        nList3.add("p");nList3.add("p2");
        List<String> vList3 = new ArrayList<String> ();
        vList3.add("Desktop");vList3.add("Desktop Web");
        Object struct3 = getStructObject("platform",nList3,vList3);
        structList.add(struct3);



        Object[] dims = new Object[2];
        dims[0] =  structList;
        dims[1] = 2;
        //dims[2] = true;
        //xploder.process(dims);
    }

    @Test
    public void testDAUExplodeXUnit() throws UDFArgumentException, HiveException {
        GenericUDTF xploder = new TaggedXUnitExplodeUDTF();
        ObjectInspector[] oiList = { ObjectInspectorFactory.getStandardListObjectInspector(getStructOI()) };
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        PrimitiveObjectInspectorFactory.javaIntObjectInspector.set(intOI,4);
        ObjectInspector boolOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.set(boolOI,true);
        ObjectInspector[] oi = new ObjectInspector[3];
        //oi[0] = ObjectoiList;
        xploder.initialize(oiList);

        ArrayList<Object> structList = new ArrayList<Object>();

        List<String> nList5 = new ArrayList<String>();
        nList5.add("is_spam");
        List<String> vList5 = new ArrayList<String> ();
        vList5.add("nonspammer-notvalidated");
        Object struct5 = getStructObject("spam",nList5,vList5);
        structList.add(struct5);

        List<String> nList0 = new ArrayList<String>();
        nList0.add("e");
        List<String> vList0 = new ArrayList<String> ();
        vList0.add("meetme");
        Object struct0 = getStructObject("event",nList0,vList0);
        structList.add(struct0);


        List<String> nList = new ArrayList<String>(1);
        nList.add(0, "bucket");
        List<String> vList = new ArrayList<String>(1);
        vList.add(0, "25-34");
        Object struct = getStructObject("age", nList, vList);
        //validateStructObject(struct, "age", nList, vList);
        structList.add(struct);



        List<String> nList4 = new ArrayList<String>();
        nList4.add("continent");nList4.add("country");nList4.add("state");
        List<String> vList4 = new ArrayList<String> ();
        vList4.add("NA");vList4.add("USA");vList4.add("CA");
        Object struct4 = getStructObject("geo",nList4,vList4);
        structList.add(struct4);

        List<String> nList3 = new ArrayList<String>();
        nList3.add("p");nList3.add("p2");
        List<String> vList3 = new ArrayList<String> ();
        vList3.add("Desktop");vList3.add("Desktop Web");
        Object struct3 = getStructObject("platform",nList3,vList3);
        structList.add(struct3);



        Object[] dims = new Object[3];
        dims[0] =  structList;
        dims[1] = 2;
        dims[2] = true;
        //xploder.process(dims);
    }
	
}