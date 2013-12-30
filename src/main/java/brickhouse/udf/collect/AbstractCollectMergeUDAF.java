package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption.*;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.getStandardObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public abstract class AbstractCollectMergeUDAF extends AbstractGenericUDAFResolver {

	public abstract Map<PrimitiveCategory, Class<? extends MergeAggBuffer>> aggBufferClasses();

	public CollectMergeUDAFEvaluator newEvaluator(PrimitiveCategory valueCategory) {
		final Class<? extends MergeAggBuffer> aggClass = aggBufferClasses().get(valueCategory);
		return new MyCollectMergeUDAFEvaluator(aggClass);
	}

	private String supportedTypes() {
		String res = "";
		for (PrimitiveCategory category : aggBufferClasses().keySet()) {
			res += ", " + category.name();
		}
		return res.substring(2);
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(1, "Expected 2 arguments");
		}
		if (parameters[1] instanceof PrimitiveTypeInfo) {
			PrimitiveTypeInfo valueInfo = (PrimitiveTypeInfo) parameters[1];
			CollectMergeUDAFEvaluator evaluator = newEvaluator(valueInfo.getPrimitiveCategory());
			if (evaluator != null) {
				return evaluator;
			} else {
				throw new UDFArgumentTypeException(1,
						"Only " + supportedTypes() + " types are supported for the 2nd argument");
			}
		} else {
			throw new UDFArgumentTypeException(1, "2nd argument must be primitive");
		}
	}


	public abstract class CollectMergeUDAFEvaluator<E> extends GenericUDAFEvaluator {

		protected ObjectInspector keyOI;
		protected PrimitiveObjectInspector valueOI;

		protected StandardMapObjectInspector internalMergeOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				keyOI = parameters[0];
				valueOI = (PrimitiveObjectInspector) parameters[1];
			} else {
				internalMergeOI = (StandardMapObjectInspector) parameters[0];
				keyOI = internalMergeOI.getMapKeyObjectInspector();
				valueOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
			}
			return getStandardMapObjectInspector(
					getStandardObjectInspector(keyOI),
					getStandardObjectInspector(valueOI, JAVA)
			);
		}

		@Override
		public abstract MergeAggBuffer<E> getNewAggregationBuffer() throws HiveException;

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((MergeAggBuffer) agg).reset();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] args) throws HiveException {
			merge((MergeAggBuffer) agg, args[0], args[1]);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return terminate(agg);
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			MergeAggBuffer myAgg = (MergeAggBuffer) agg;
			for (Map.Entry<?, ?> e : internalMergeOI.getMap(partial).entrySet()) {
				merge(myAgg, e.getKey(), e.getValue());
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			return ((MergeAggBuffer) agg).copy();
		}

		@SuppressWarnings("unchecked")
		private void merge(MergeAggBuffer agg, Object key, Object value) {
			Object keyCopy = ObjectInspectorUtils.copyToStandardJavaObject(key, keyOI);
			E primValue = (E) valueOI.getPrimitiveJavaObject(value);
			((MergeAggBuffer<E>) agg).merge(keyCopy, primValue);
		}
	}


	public static interface MergeAggBuffer<V> extends AggregationBuffer {

		void reset();

		V mergeValues(V left, V right);

		V defaultValue();

		void merge(Object key, V value);

		Map<Object, V> copy();
	}

	public static abstract class HashMapMergeAggBuffer<V>
			extends HashMap<Object, V> implements MergeAggBuffer<V> {

		@Override
		public void reset() {
			clear();
		}

		@Override
		public void merge(Object key, V value) {
			if (containsKey(key)) {
				V oldValue = get(key);
				V newValue = mergeValues(oldValue, value);
				if (!oldValue.equals(newValue)) {
					put(key, newValue);
				}
			} else {
				put(key, mergeValues(defaultValue(), value));
			}
		}

		@Override
		public Map<Object, V> copy() {
			return new HashMap<Object, V>(this);
		}
	}

	public static abstract class BooleanMergeAggBuffer extends HashMapMergeAggBuffer<Boolean> {
		@Override
		public Boolean defaultValue() {
			return false;
		}
	}

	public static abstract class ByteMergeAggBuffer extends HashMapMergeAggBuffer<Byte> {
		@Override
		public Byte defaultValue() {
			return 0;
		}
	}

	public static abstract class ShortMergeAggBuffer extends HashMapMergeAggBuffer<Short> {
		@Override
		public Short defaultValue() {
			return 0;
		}
	}

	public static abstract class IntMergeAggBuffer extends HashMapMergeAggBuffer<Integer> {
		@Override
		public Integer defaultValue() {
			return 0;
		}
	}

	public static abstract class LongMergeAggBuffer extends HashMapMergeAggBuffer<Long> {
		@Override
		public Long defaultValue() {
			return 0L;
		}
	}

	public static abstract class FloatMergeAggBuffer extends HashMapMergeAggBuffer<Float> {
		@Override
		public Float defaultValue() {
			return 0.0f;
		}
	}

	public static abstract class DoubleMergeAggBuffer extends HashMapMergeAggBuffer<Double> {
		@Override
		public Double defaultValue() {
			return 0.0;
		}
	}

	public static abstract class StringMergeAggBuffer extends HashMapMergeAggBuffer<String> {
		@Override
		public String defaultValue() {
			return "";
		}
	}

	public class MyCollectMergeUDAFEvaluator extends CollectMergeUDAFEvaluator {
		private final Class<? extends MergeAggBuffer> aggClass;

		public MyCollectMergeUDAFEvaluator(Class<? extends MergeAggBuffer> aggClass) {
			this.aggClass = aggClass;
		}

		@Override
		public MergeAggBuffer getNewAggregationBuffer() throws HiveException {
			try {
				return aggClass.newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
