
package brickhouse.udf.xunit;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.*;


/**
 *  Return a list of parent segments(dimensions) for event and ab test yPaths. For example given the following
 *  xunit: /age/bucket=18-24,/cohort/signup=1,/event/e=login_detail,/geo/continent=EU/country=DE"
 *  getsegments() function will return struct</event/e=login_detail, [/age,/cohort,/geo]>
 */

public class GetYPathSegmentsUDF extends GenericUDF {
    private static final List<String> SEGMENTABLE_YPATH_LIST = Lists.newArrayList("/event", "/ab");
    private static final String CUSTOM_SEGMENT = "/custom";

    private StringObjectInspector elementOI;

    @Override
    public String getDisplayString(String[] strings) {
        assert( strings.length>0 );

        StringBuilder sb = new StringBuilder();
        sb.append("getsegments(");
        sb.append( strings[0]);
        sb.append(")");

        return sb.toString();
    }

    /**
     * This is what the initialize() method does:
     *  Verify that the input is of the type expected
     *  Set up the ObjectInspectors for the input in global variables
     *  Return the ObjectInspector for the output
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("getsegments() accepts exactly 1 argument.");
        }

        // Check we received the right object types.
        ObjectInspector arg1 = arguments[0];
        if (! (arg1 instanceof StringObjectInspector)) {
            throw new UDFArgumentException("getsegments() udf argument must be of type String");
        }

        this.elementOI = (StringObjectInspector) arg1;

        // Define the field names for the return struct<> and their types
        ArrayList structFieldNames = new ArrayList();
        ArrayList structFieldObjectInspectors = new ArrayList();

        structFieldNames.add("yPath");
        structFieldNames.add("segments");

        structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector );
        structFieldObjectInspectors.add( ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector));

        // Set up the output object inspector for the struct<string, list<string>>
        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(
                structFieldNames, structFieldObjectInspectors);

        return soi;
    }

   @Description(name = "getsegments",
            value = "_FUNC_(/age/bucket=18-24,/cohort/signup=1,/event/e=login_detail) - Generates " +
                    " struct containing segmentable ypath and segments seperated by comma")
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // Should be exactly one argument
        if( arguments.length != 1 )
            return null;

        // If passed a null, return a null
        if( arguments[0].get() == null )
            return null;


        String segmentableXUnit = null;
        List<String> segments = new ArrayList<String>();

        String xunit = elementOI.getPrimitiveJavaObject(arguments[0].get());
        for( String ypath: xunit.split(",")) {
            String[] ypathParts = ypath.split("/");
            String parentPart = ypath.length() > 1 ? "/"+ ypathParts[1] : null;
            if (SEGMENTABLE_YPATH_LIST.contains(parentPart)) {
                segmentableXUnit = ypath;
            } else {
                // Only add segments of the ypath that have more than 1 path separator (/)
                if(ypathParts.length < 3) {
                    continue;
                }

                String[] segmentAndValue = ypathParts[2].split("=");
                if (CUSTOM_SEGMENT.contains(parentPart)) {
                    segments.add(parentPart + "/" + segmentAndValue[0]);
                } else {
                    segments.add(parentPart);
                }
            }
        }

        if(segmentableXUnit != null && segments.size() > 0) {
            Object[] struct = new Object[2];
            struct[0] = segmentableXUnit;
            struct[1] = segments;
            return struct;
        }

	    return null;
	}
}
