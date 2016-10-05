package brickhouse.udf.collect;

import brickhouse.udf.collect.CollectDistinctUDAF.SetCollectUDAFEvaluator;
import brickhouse.udf.collect.CollectMaxUDAF.MapCollectMaxUDAFEvaluator;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CollectDistinctUDFTest {

	private CollectDistinctUDAF udf;

	@Before
	public void before() {
		udf = new CollectDistinctUDAF();
	}

	@Test(expected = UDFArgumentTypeException.class)
	public void testGetEvaluatorNoArg() throws SemanticException {
		udf.getEvaluator(new StructTypeInfo[0]);
	}

	@Test(expected = UDFArgumentTypeException.class)
	public void testGetEvaluatorTwoArg() throws SemanticException {
		List<String> structFieldNames = new ArrayList<String>();
		structFieldNames.add("field1");
		structFieldNames.add("field2");

		List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils
				.getTypeInfoFromObjectInspector(ObjectInspectorFactory
						.getStandardStructObjectInspector(structFieldNames,
								objectInspectors));

		List<String> structFieldNames2 = new ArrayList<String>();
		structFieldNames2.add("field1");
		structFieldNames2.add("field2");

		List<ObjectInspector> objectInspectors2 = new ArrayList<ObjectInspector>();
		objectInspectors2
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		objectInspectors2
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		StructTypeInfo structTypeInfo2 = (StructTypeInfo) TypeInfoUtils
				.getTypeInfoFromObjectInspector(ObjectInspectorFactory
						.getStandardStructObjectInspector(structFieldNames2,
								objectInspectors2));

		udf.getEvaluator(new StructTypeInfo[] { structTypeInfo, structTypeInfo2 });
	}

	@Test
	public void testInitializeWithOneArguments() throws SemanticException {
		List<String> structFieldNames = new ArrayList<String>();
		structFieldNames.add("field1");
		structFieldNames.add("field2");

		List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils
				.getTypeInfoFromObjectInspector(ObjectInspectorFactory
						.getStandardStructObjectInspector(structFieldNames,
								objectInspectors));
		udf.getEvaluator(new StructTypeInfo[] { structTypeInfo });
	}

	@Test
	public void testInit() throws HiveException {

		SetCollectUDAFEvaluator maxEval = new SetCollectUDAFEvaluator();

		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("field1");
		fieldNames.add("field2");

		List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		objectInspectors
				.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StructObjectInspector keyOI = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, objectInspectors);
		// WritableIntObjectInspector valOI = (WritableIntObjectInspector)
		// PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

		maxEval.init(Mode.PARTIAL1, new ObjectInspector[] { keyOI });
	}
}
