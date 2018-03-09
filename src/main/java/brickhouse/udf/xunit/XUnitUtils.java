package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import java.util.ArrayList;
import java.util.List;

/**
 *  A set of static utility functions for dealing with XUnit structures
 */
public final class XUnitUtils {


    /**
     *   Use an ObjectInspector to "inspect" incoming arguments,
     *    to produce an XUnitDesc.
     *
     *   Support polymorphism so that either a String can be
     *    passed in, or a named_struct with XUnit values.
     *
     * @param o
     * @param inspector
     * @return
     */
    static public XUnitDesc InspectXUnit(Object o, ObjectInspector inspector) throws HiveException {
        if( inspector instanceof StringObjectInspector) {
           String xunitStr = ((StringObjectInspector)((StringObjectInspector) inspector)).getPrimitiveJavaObject(o);
           XUnitDesc xunit = XUnitDesc.ParseXUnit( xunitStr) ;
           return xunit;
        } else if( inspector instanceof StructObjectInspector) {
            StructObjectInspector xunitStructInspector = (StructObjectInspector) inspector;

            StructField isGlobalSF = xunitStructInspector.getStructFieldRef("is_global");
            boolean isGlobal = ((BooleanObjectInspector) isGlobalSF.getFieldObjectInspector()).get( xunitStructInspector.getStructFieldData(o, isGlobalSF));
            if( isGlobal) {
                return XUnitDesc.GlobalXUnit;
            } else {
                StructField ypathsSF = xunitStructInspector.getStructFieldRef("ypaths");
                ListObjectInspector ypathListInspector = (ListObjectInspector) ypathsSF.getFieldObjectInspector();
                ObjectInspector ypathInspector = ypathListInspector.getListElementObjectInspector();

                Object ypathListObj = xunitStructInspector.getStructFieldData( o, ypathsSF);

                List<Object> ypathObjList = (List<Object>) ypathListInspector.getList( ypathListObj);

                List<YPathDesc> ypList = new ArrayList<YPathDesc>();
                for(Object ypathObj : ypathObjList) {
                    YPathDesc ypath = InspectYPath(ypathObj, ypathInspector);
                    ypList.add( ypath);
                }

                return new XUnitDesc( ypList.toArray( new YPathDesc[ypList.size()]));
            }

        } else {
            throw new HiveException(" Can't interpret XUnit with ObjectInspector  " + inspector);
        }

    }

    static public ObjectInspector ValidateXUnitObjectInspector( ObjectInspector inspector, String errorMessage) throws UDFArgumentException {

        //// Make sure names are there
        if( inspector.getCategory() == ObjectInspector.Category.STRUCT) {
            StructObjectInspector xunitStructInspector = (StructObjectInspector)inspector;
            if (xunitStructInspector.getStructFieldRef("is_global") == null
                    || xunitStructInspector.getStructFieldRef("ypaths") == null) {
                throw new UDFArgumentException(errorMessage);
            }
            StructField ypathsSF = xunitStructInspector.getStructFieldRef("ypaths");
            if( ypathsSF.getFieldObjectInspector().getCategory() != ObjectInspector.Category.LIST) {
                ListObjectInspector ypathsListInspector = (ListObjectInspector)ypathsSF.getFieldObjectInspector();
                ValidateYPathObjectInspector( ypathsListInspector.getListElementObjectInspector(), errorMessage);
            }
            return xunitStructInspector;
        } else if( inspector instanceof StringObjectInspector) {
            return inspector;
        } else {
            throw new UDFArgumentException(errorMessage);
        }
    }

    /**
     *  Translate the XUnitDesc to an Object
     *   which can be returned from a GenericeUDF
     *    which can be interpreted as a valid XUnit
     * @param xunit
     * @return
     */
    static public Object StructObjectForXUnit( XUnitDesc xunit) {
        if(xunit == null) {
            return null;
        }
        if( xunit.isGlobal()) {
            /// The Global XUnit has no attributes
            return new Object[] {
                    true,
                    new Object[] { }
            };
        } else {
            Object[] attrObjects = new Object[ xunit.getYPaths().length ];
            for( int i=0; i<attrObjects.length; ++i) {
                attrObjects[i] = StructObjectForYPath(xunit.getYPaths()[i] );
            }
            return new Object[] {
                    false,
                    attrObjects
            };
        }
    }


    /**
     *  Translate the YPathDesc to an Object
     *   which can be returned from a GenericeUDF
     *   which can be interpreted as a valid YPath
     *
     * @param ypath
     *
     * **/
    static public Object StructObjectForYPath( YPathDesc ypath) {
        if(ypath == null) {
            return ypath;
        }

        Object[] attributes = new Object[ypath.numLevels()];
        String[] attrNames = ypath.getAttributeNames();
        String[] attrValues = ypath.getAttributeValues();
        for(int i=0; i<= attributes.length -1; ++i) {
            attributes[i]  =  new Object[] { attrNames[i], attrValues[i] };
        }

        //// Return YPath as neste Object Arrau
        return new Object[]{
                ypath.getDimName(),
                attributes
        };

    }



    static public YPathDesc InspectYPath( Object o, ObjectInspector inspector) throws HiveException {
        if( inspector instanceof StringObjectInspector) {
            String ypathStr = ((StringObjectInspector)inspector).getPrimitiveJavaObject(o);

            return YPathDesc.ParseYPath( ypathStr);
        } else if( inspector instanceof StructObjectInspector) {
            StructObjectInspector ypathInspector = (StructObjectInspector)inspector;
            List<Object> ypathFields = ypathInspector.getStructFieldsDataAsList( o);

            StructField dimSF = ypathInspector.getStructFieldRef("dim");
            String dimName = ((StringObjectInspector)dimSF.getFieldObjectInspector()).getPrimitiveJavaObject( ypathFields.get(0));

            StructField attrsSF = ypathInspector.getStructFieldRef("attributes");
            List<Object> attrObjs = (List<Object>)((ListObjectInspector)attrsSF.getFieldObjectInspector() ).getList( ypathFields.get(1));

            StructObjectInspector attrInspector = (StructObjectInspector)((ListObjectInspector) attrsSF.getFieldObjectInspector()).getListElementObjectInspector();
            List<String> attrNames = new ArrayList<String>();
            List<String> attrValues = new ArrayList<String>();
            StructField attrNameSF = attrInspector.getStructFieldRef("attribute_name");
            StructField attrValueSF = attrInspector.getStructFieldRef("attribute_value");

            for(Object attrObj : attrObjs) {
                List<Object> attrKV = attrInspector.getStructFieldsDataAsList( attrObj);
               String attrName =  ((StringObjectInspector)attrNameSF.getFieldObjectInspector()).getPrimitiveJavaObject(attrKV.get(0));
               String attrValue =  ((StringObjectInspector)attrValueSF.getFieldObjectInspector()).getPrimitiveJavaObject(attrKV.get(1));
               attrNames.add(attrName);
               attrValues.add( attrValue);
            }

            return new YPathDesc( dimName, attrNames.toArray(new String[attrNames.size()]), attrValues.toArray( new String[attrValues.size()]));
        } else {
            throw new HiveException(" Unable to extract YPath from ObjectInspector " + inspector);
        }
    }

    static public ObjectInspector ValidateYPathObjectInspector( ObjectInspector inspector, String errorMessage) throws UDFArgumentException {

      ///// XXX TODO

        return inspector;



    }


    /**
     *  Return the StandardStructObjectInspector corresponding to a YPath struct
     * @return
     */
    static public StructObjectInspector GetYPathInspector() {
        List<String> ypathFieldsNames = new ArrayList<String>();
        List<ObjectInspector>  ypathFieldInspectors = new ArrayList<ObjectInspector>();

        ypathFieldsNames.add("dim");
        ypathFieldInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        List<String> attrFieldNames = new ArrayList<String>();
        List<ObjectInspector> attrFieldInspectors = new ArrayList<ObjectInspector>();

        attrFieldNames.add("attribute_name");
        attrFieldInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        attrFieldNames.add("attribute_value");
        attrFieldInspectors.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        ObjectInspector attrInspector = ObjectInspectorFactory.getStandardStructObjectInspector(attrFieldNames, attrFieldInspectors);
        ListObjectInspector attrListInspector = ObjectInspectorFactory.getStandardListObjectInspector( attrInspector);

        ypathFieldsNames.add("attributes");
        ypathFieldInspectors.add( attrListInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(ypathFieldsNames, ypathFieldInspectors);
    }


    static public StructObjectInspector GetXUnitInspector() {

        ArrayList<String> xunitFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> xunitFieldInspectors = new ArrayList<ObjectInspector>();


        xunitFieldNames.add("is_global");
        xunitFieldInspectors.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

        xunitFieldNames.add("ypaths");
        ObjectInspector ypathsInpector = ObjectInspectorFactory.getStandardListObjectInspector( GetYPathInspector() );
        xunitFieldInspectors.add( ypathsInpector);


        return ObjectInspectorFactory.getStandardStructObjectInspector(xunitFieldNames,xunitFieldInspectors);
    }


    /**
     *  Consolidate the logic for getting a List of Strings from a GenericUDF Object
     * @param o
     * @param listInspector
     * @return
     */
    public static List<String> InspectStringList( Object o, ListObjectInspector listInspector) {
        //// Need to translate
        StringObjectInspector stringInspector = (StringObjectInspector)listInspector.getListElementObjectInspector();
        ArrayList<String> retVal = new ArrayList<String>();
        int len = listInspector.getListLength( o);
        for( int i = 0; i<= len -1; ++i) {
            Object strObj = listInspector.getListElement( o, i);
            String  str = stringInspector.getPrimitiveJavaObject( strObj);
            retVal.add( str);
        }

        return retVal;
    }


    static public String DisplayString( String udfName, String[] args) {
        StringBuilder sb = new StringBuilder( udfName);
        sb.append("( ");
        for( int i=0; i<args.length -1; ++i) {
            sb.append(args[i]);
            if( i != args.length -1 ) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
