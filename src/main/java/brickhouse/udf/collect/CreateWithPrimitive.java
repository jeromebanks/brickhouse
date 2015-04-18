package brickhouse.udf.collect;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;

import java.sql.Timestamp;

public abstract class CreateWithPrimitive {
    public static CreateWithPrimitive getCreate(PrimitiveObjectInspector oi) {
        if (oi instanceof SettableBooleanObjectInspector) {
            final SettableBooleanObjectInspector settableOI =
                    (SettableBooleanObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Boolean) o);
                }
            };
        } else if (oi instanceof SettableByteObjectInspector) {
            final SettableByteObjectInspector settableOI =
                    (SettableByteObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Byte) o);
                }
            };
        } else if (oi instanceof SettableShortObjectInspector) {
            final SettableShortObjectInspector settableOI =
                    (SettableShortObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Short) o);
                }
            };
        } else if (oi instanceof SettableIntObjectInspector) {
            final SettableIntObjectInspector settableOI =
                    (SettableIntObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Integer) o);
                }
            };
        } else if (oi instanceof SettableLongObjectInspector) {
            final SettableLongObjectInspector settableOI =
                    (SettableLongObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Long) o);
                }
            };
        } else if (oi instanceof SettableFloatObjectInspector) {
            final SettableFloatObjectInspector settableOI =
                    (SettableFloatObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Float) o);
                }
            };
        } else if (oi instanceof SettableDoubleObjectInspector) {
            final SettableDoubleObjectInspector settableOI =
                    (SettableDoubleObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Double) o);
                }
            };
        } else if (oi instanceof SettableStringObjectInspector) {
            final SettableStringObjectInspector settableOI =
                    (SettableStringObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((String) o);
                }
            };
        } else if (oi instanceof SettableTimestampObjectInspector) {
            final SettableTimestampObjectInspector settableOI =
                    (SettableTimestampObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((Timestamp) o);
                }
            };
        } else if (oi instanceof SettableBinaryObjectInspector) {
            final SettableBinaryObjectInspector settableOI =
                    (SettableBinaryObjectInspector) oi;
            return new CreateWithPrimitive() {
                @Override
                public Object create(Object o) {
                    if (o == null) return null;
                    return settableOI.create((byte[]) o);
                }
            };
        } else {
            return null;
        }
    }

    public abstract Object create(Object primitive);
}
