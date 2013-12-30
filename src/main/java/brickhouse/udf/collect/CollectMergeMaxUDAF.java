package brickhouse.udf.collect;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;

public class CollectMergeMaxUDAF extends AbstractCollectMergeUDAF {
	public CollectMergeMaxUDAF() {}

	@Override
	public Map<PrimitiveCategory, Class<? extends MergeAggBuffer>> aggBufferClasses() {
		return new HashMap<PrimitiveCategory, Class<? extends MergeAggBuffer>>() {{
			put(BOOLEAN, BooleanMergeMaxAggBuffer.class);
			put(BYTE, ByteMergeMaxAggBuffer.class);
			put(SHORT, ShortMergeMaxAggBuffer.class);
			put(INT, IntMergeMaxAggBuffer.class);
			put(LONG, LongMergeMaxAggBuffer.class);
			put(FLOAT, FloatMergeMaxAggBuffer.class);
			put(DOUBLE, DoubleMergeMaxAggBuffer.class);
			put(STRING, StringMergeMaxAggBuffer.class);
			put(TIMESTAMP, TimestampMergeMaxAggBuffer.class);
		}};
	}

	public static class BooleanMergeMaxAggBuffer extends BooleanMergeAggBuffer {
		public BooleanMergeMaxAggBuffer() {}

		@Override
		public Boolean mergeValues(Boolean left, Boolean right) {
			return left || right;
		}
	}

	public static class ByteMergeMaxAggBuffer extends ByteMergeAggBuffer {
		public ByteMergeMaxAggBuffer() {}

		@Override
		public Byte defaultValue() {
			return Byte.MIN_VALUE;
		}

		@Override
		public Byte mergeValues(Byte left, Byte right) {
			return (byte) Math.max(left, right);
		}
	}

	public static class ShortMergeMaxAggBuffer extends ShortMergeAggBuffer {
		public ShortMergeMaxAggBuffer() {}

		@Override
		public Short defaultValue() {
			return Short.MIN_VALUE;
		}

		@Override
		public Short mergeValues(Short left, Short right) {
			return (short) Math.max(left, right);
		}
	}

	public static class IntMergeMaxAggBuffer extends IntMergeAggBuffer {
		public IntMergeMaxAggBuffer() {}

		@Override
		public Integer defaultValue() {
			return Integer.MIN_VALUE;
		}

		@Override
		public Integer mergeValues(Integer left, Integer right) {
			return Math.max(left, right);
		}
	}

	public static class LongMergeMaxAggBuffer extends LongMergeAggBuffer {
		public LongMergeMaxAggBuffer() {}

		@Override
		public Long defaultValue() {
			return Long.MIN_VALUE;
		}

		@Override
		public Long mergeValues(Long left, Long right) {
			return Math.max(left, right);
		}
	}

	public static class FloatMergeMaxAggBuffer extends FloatMergeAggBuffer {
		public FloatMergeMaxAggBuffer() {}

		@Override
		public Float defaultValue() {
			return Float.MIN_VALUE;
		}

		@Override
		public Float mergeValues(Float left, Float right) {
			return Math.max(left, right);
		}
	}

	public static class DoubleMergeMaxAggBuffer extends DoubleMergeAggBuffer {
		public DoubleMergeMaxAggBuffer() {}

		@Override
		public Double defaultValue() {
			return Double.MIN_VALUE;
		}

		@Override
		public Double mergeValues(Double left, Double right) {
			return Math.max(left, right);
		}
	}

	public static class StringMergeMaxAggBuffer extends StringMergeAggBuffer {
		public StringMergeMaxAggBuffer() {}

		@Override
		public String mergeValues(String left, String right) {
			return left.compareTo(right) <= 0 ? left : right;
		}
	}

	public static class TimestampMergeMaxAggBuffer extends HashMapMergeAggBuffer<Timestamp> {
		public TimestampMergeMaxAggBuffer() {}

		@Override
		public Timestamp mergeValues(Timestamp left, Timestamp right) {
			return left.compareTo(right) <= 0 ? left : right;
		}

		@Override
		public Timestamp defaultValue() {
			return MIN;
		}

		private static final Timestamp MIN = new Timestamp(Long.MIN_VALUE);
	}
}
