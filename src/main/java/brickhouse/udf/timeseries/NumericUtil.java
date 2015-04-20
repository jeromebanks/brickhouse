package brickhouse.udf.timeseries;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

public final class NumericUtil {

    public static boolean isNumericCategory(PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Cast the output of a Numeric ObjectInspector
     * to a double
     *
     * @param objInsp
     * @return
     */
    public static double getNumericValue(PrimitiveObjectInspector objInsp, Object val) {
        switch (objInsp.getPrimitiveCategory()) {
            case DOUBLE:
                return ((DoubleObjectInspector) objInsp).get(val);
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                Number num = (Number) objInsp.getPrimitiveJavaObject(val);
                return num.doubleValue();
            default:
                return 0.0;
        }
    }


    /**
     * Cast a double to an object required by the ObjectInspector
     * associated with the given PrimitiveCategory
     *
     * @param val
     * @param cat
     * @return
     */
    public static Object castToPrimitiveNumeric(double val, PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
                return new Double(val);
            case FLOAT:
                return new Float((float) val);
            case LONG:
                return new Long((long) val);
            case INT:
                return new Integer((int) val);
            case SHORT:
                return new Short((short) val);
            case BYTE:
                return new Byte((byte) val);
            default:
                return null;
        }
    }

}
