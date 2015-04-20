package brickhouse.udf.collect;


import brickhouse.udf.collect.CollectMaxUDAF.MapCollectMaxUDAFEvaluator;
import brickhouse.udf.collect.CollectMaxUDAF.MapCollectMaxUDAFEvaluator.MapAggBuffer;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Map;

public class CollectMaxTest {


    ///@Test
    public void testCollectMaxAggBuffer() throws HiveException {
        MapCollectMaxUDAFEvaluator maxEval = new MapCollectMaxUDAFEvaluator(true);

        WritableStringObjectInspector keyOI = (WritableStringObjectInspector) PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);
        WritableIntObjectInspector valOI = (WritableIntObjectInspector) PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);


        maxEval.init(Mode.PARTIAL1, new ObjectInspector[]{keyOI, valOI});


        MapAggBuffer buffer = (MapAggBuffer) maxEval.getNewAggregationBuffer();

        Text key = new Text();
        IntWritable val = new IntWritable();
        for (int i = 0; i < 100; i++) {
            key.set(" Key # " + i);
            val.set(i);
            buffer.addValue(key, val);
        }

        Map<Text, IntWritable> valueMap = (Map<Text, IntWritable>) buffer.getValueMap();
        int lastValue = 99;
        for (Map.Entry<Text, IntWritable> entry : valueMap.entrySet()) {
            System.out.println(" key is " + entry.getKey());
            System.out.println(" val is " + entry.getValue());
            Assert.assertTrue(entry.getValue().get() == lastValue);
            lastValue--;
        }

        Assert.assertEquals(CollectMaxUDAF.DEFAULT_MAX_VALUES, valueMap.size());

        buffer.reset();
        for (int i = 0; i < 1000; i++) {
            int rand = (int) (Math.random() * 10000.00);
            key.set(" Key # " + i);
            val.set(rand);
            buffer.addValue(key, val);
        }

        valueMap = (Map<Text, IntWritable>) buffer.getValueMap();
        lastValue = Integer.MAX_VALUE;
        for (Map.Entry<Text, IntWritable> entry : valueMap.entrySet()) {
            System.out.println(" key is " + entry.getKey());
            System.out.println(" val is " + entry.getValue());
            Assert.assertTrue(entry.getValue().get() <= lastValue);
        }

        Assert.assertEquals(CollectMaxUDAF.DEFAULT_MAX_VALUES, valueMap.size());

    }

    @Test
    public void testCollectMinAggBuffer() throws HiveException {
        MapCollectMaxUDAFEvaluator maxEval = new MapCollectMaxUDAFEvaluator(false);

        WritableStringObjectInspector keyOI = (WritableStringObjectInspector) PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);
        WritableIntObjectInspector valOI = (WritableIntObjectInspector) PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);


        maxEval.init(Mode.PARTIAL1, new ObjectInspector[]{keyOI, valOI});


        MapAggBuffer buffer = (MapAggBuffer) maxEval.getNewAggregationBuffer();

        Text key = new Text();
        IntWritable val = new IntWritable();
        for (int i = 100; i >= 0; i--) {
            key.set(" Key # " + i);
            val.set(i);
            buffer.addValue(key, val);
        }

        Map<Text, IntWritable> valueMap = (Map<Text, IntWritable>) buffer.getValueMap();
        int firstValue = 0;
        for (Map.Entry<Text, IntWritable> entry : valueMap.entrySet()) {
            System.out.println(" key is " + entry.getKey());
            System.out.println(" val is " + entry.getValue());
            Assert.assertTrue(entry.getValue().get() == firstValue);
            firstValue++;
        }

        Assert.assertEquals(CollectMaxUDAF.DEFAULT_MAX_VALUES, valueMap.size());

        buffer.reset();
        for (int i = 0; i < 100; i++) {
            key.set(" Key # " + i);
            val.set(i);
            buffer.addValue(key, val);
        }

        valueMap = (Map<Text, IntWritable>) buffer.getValueMap();
        firstValue = 0;
        for (Map.Entry<Text, IntWritable> entry : valueMap.entrySet()) {
            System.out.println(" key is " + entry.getKey());
            System.out.println(" val is " + entry.getValue());
            Assert.assertTrue(entry.getValue().get() == firstValue);
            firstValue++;
        }

        Assert.assertEquals(CollectMaxUDAF.DEFAULT_MAX_VALUES, valueMap.size());

        buffer.reset();
        for (int i = 0; i < 1000; i++) {
            int rand = (int) (Math.random() * 10000.00);
            key.set(" Key # " + i);
            val.set(rand);
            buffer.addValue(key, val);
        }

        int lastValue = Integer.MIN_VALUE;
        valueMap = (Map<Text, IntWritable>) buffer.getValueMap();
        for (Map.Entry<Text, IntWritable> entry : valueMap.entrySet()) {
            System.out.println(" key is " + entry.getKey());
            System.out.println(" val is " + entry.getValue());
            Assert.assertTrue(entry.getValue().get() >= lastValue);
        }

        Assert.assertEquals(CollectMaxUDAF.DEFAULT_MAX_VALUES, valueMap.size());

    }


}

	
