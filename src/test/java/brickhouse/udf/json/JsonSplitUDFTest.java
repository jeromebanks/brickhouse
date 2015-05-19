package brickhouse.udf.json;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author andrey.myatlyuk@lithium.com
 */
public class JsonSplitUDFTest {

	private JsonSplitUDF udf;

	@Before
	public void before() {
		udf = new JsonSplitUDF();
	}

	@Test
	public void testEvaluate() throws Exception {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{stringOi});

		String value = "[\"a\", \"b\"]";

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(value)});
		assertEquals(2, resultOi.getList(result).size());
	}

	@Test
	public void testEvaluateWithEmptyList() throws Exception {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{stringOi});

		String value = "[]";

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(value)});
		assertTrue(resultOi.getList(result).isEmpty());
	}

	@Test(expected = HiveException.class)
	public void testEvaluateWithEmptyString() throws Exception {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		udf.initialize(new ObjectInspector[]{stringOi});

		String value = "";

		udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(value)});
	}

	@Test
	public void testEvaluateWithNull() throws Exception {
		ObjectInspector stringOi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		StandardListObjectInspector resultOi = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{stringOi});

		Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(null)});
		assertNull(resultOi.getList(result));
	}
}
