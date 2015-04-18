package brickhouse.udf.collect;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.getStandardObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public abstract class AbstractCollectMergeUDAF extends AbstractGenericUDAFResolver {

    public abstract Map<PrimitiveCategory, Class<? extends CollectMergeUDAFEvaluator>> evaluators();

    public CollectMergeUDAFEvaluator newEvaluator(PrimitiveCategory valueCategory) {
        Class<? extends CollectMergeUDAFEvaluator> evaluatorClass = evaluators().get(valueCategory);
        try {
            return evaluatorClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private String supportedTypes() {
        String res = "";
        for (PrimitiveCategory category : evaluators().keySet()) {
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


    public static abstract class CollectMergeUDAFEvaluator<E> extends GenericUDAFEvaluator {

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
            Object keyCopy = ObjectInspectorUtils.copyToStandardObject(key, keyOI);
            E primValue = (E) valueOI.getPrimitiveJavaObject(value);
            ((MergeAggBuffer<E>) agg).merge(keyCopy, primValue);
        }
    }


    public static interface MergeAggBuffer<V> extends AggregationBuffer {

        void reset();

        V mergeValues(V left, V right);

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
            V oldValue = get(key);
            if (oldValue != null) {
                // don't merge with null
                // this method should be overridden in subclasses if special treatment of nulls is needed
                if (value != null) {
                    V newValue = mergeValues(oldValue, value);
                    if (!oldValue.equals(newValue)) {
                        put(key, newValue);
                    }
                }
            } else {
                // if key is absent in the map yet of previous value is null
                put(key, value);
            }
        }

        @Override
        public Map<Object, V> copy() {
            return new HashMap<Object, V>(this);
        }
    }
}
