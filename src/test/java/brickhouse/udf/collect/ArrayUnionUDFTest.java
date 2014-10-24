package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArrayUnionUDFTest {

	private ArrayUnionUDF udf;

	@Before
	public void before() {
		udf = new ArrayUnionUDF();
	}

	@Test(expected = UDFArgumentException.class)
	public void testInitializeWithNoArguments() throws UDFArgumentException {
		udf.initialize(new ObjectInspector[0]);
	}

	@Test(expected = UDFArgumentException.class)
	public void testInitializeWithOneArgument() throws UDFArgumentException {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
		udf.initialize(new ObjectInspector[]{listOi});
	}

	@Test
	public void testInitializeWithTwoArguments() throws UDFArgumentException {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(stringOi);
		udf.initialize(new ObjectInspector[]{listOi, listOi});
	}

	/**
	 * {1, 2} ∪ {1, 2} = {1, 2}
	 */
	@Test
	public void testEvaluateWithSameElements() throws HiveException {
		ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, listOi});

		List<Integer> one = new ArrayList<Integer>();
		one.add(1);
		one.add(2);

		List<Integer> two = new ArrayList<Integer>();
		two.add(1);
		two.add(2);

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(two)});
		assertEquals(2, resultOi.getListLength(result));
		assertTrue(resultOi.getList(result).contains(1));
		assertTrue(resultOi.getList(result).contains(2));
	}

	/**
	 * {1, 2} ∪ {2, 3} = {1, 2, 3}
	 */
	@Test
	public void testEvaluateWithTwoArrays() throws HiveException {
		ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, listOi});

		List<Integer> one = new ArrayList<Integer>();
		one.add(1);
		one.add(2);

		List<Integer> two = new ArrayList<Integer>();
		two.add(2);
		two.add(3);

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(two)});
		assertEquals(3, resultOi.getListLength(result));
		assertTrue(resultOi.getList(result).contains(1));
		assertTrue(resultOi.getList(result).contains(2));
		assertTrue(resultOi.getList(result).contains(3));
	}

	/**
	 * {1, 2} ∪ {2, 5} u {3, 4} = {1, 2, 3, 4, 5}
	 */
	@Test
	public void testEvaluateWithThreeArrays() throws HiveException {
		ObjectInspector intOi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		ObjectInspector listOi = ObjectInspectorFactory.getStandardListObjectInspector(intOi);
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOi, listOi, listOi});

		List<Integer> one = new ArrayList<Integer>();
		one.add(1);
		one.add(2);

		List<Integer> two = new ArrayList<Integer>();
		two.add(2);
		two.add(5);

		List<Integer> three = new ArrayList<Integer>();
		two.add(3);
		two.add(4);

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(one), new DeferredJavaObject(two), new DeferredJavaObject(three)});
		assertEquals(5, resultOi.getListLength(result));
		assertTrue(resultOi.getList(result).contains(1));
		assertTrue(resultOi.getList(result).contains(2));
		assertTrue(resultOi.getList(result).contains(3));
		assertTrue(resultOi.getList(result).contains(4));
		assertTrue(resultOi.getList(result).contains(5));
	}
}
