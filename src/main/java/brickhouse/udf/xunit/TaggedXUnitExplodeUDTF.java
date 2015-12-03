package brickhouse.udf.xunit;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */


import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import brickhouse.udf.counter.IncrCounterUDF;

/**
 *   Generate XUnits for a set of dimensional values
 *
 *   Users pass in an array of Hive structures representing the
 *     values
 */
@Description(
        name="xunit_explode",
        value="_FUNC_(array<struct<dim:string,attr_names:array<string>,attr_values:array<string>>, int, boolean) - ",
        extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo;")
public class TaggedXUnitExplodeUDTF extends GenericUDTF {
    private static final Logger LOG = Logger.getLogger( XUnitExplodeUDTF.class);

    private static final String GLOBAL_UNIT = "/G";

    private ListObjectInspector listInspector;
    private StructObjectInspector structInspector;
    private StructField dimField;
    private StructField attrNamesField;
    private StructField attrValuesField;
    private ListObjectInspector attrNamesInspector;
    private StringObjectInspector attrNameInspector;
    private ListObjectInspector attrValuesInspector;
    private StringObjectInspector attrValueInspector;
    private String[] xunitFieldArr = new String[1];

    private IntObjectInspector maxDimInspector = null;
    private int maxDims = -1;

    private BooleanObjectInspector globalFlagInspector;

    private Reporter reporter;

    private Reporter getReporter() throws ClassNotFoundException, NoSuchMethodException, SecurityException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if(reporter == null) {
            reporter = IncrCounterUDF.GetReporter();
        }
        return reporter;
    }


    private void incrCounter( String counterName, long counter) {
        try {
            getReporter().incrCounter("XUnitExplode", counterName, counter);
        } catch ( Exception exc) {
            LOG.error("Error incrementing counter " + counterName, exc);
        }
    }

    @Override
    public void close() throws HiveException {

    }


    public XUnitDesc fromPath(YPathDesc yp ) {
        return new XUnitDesc( yp);
    }

    public XUnitDesc addYPath(XUnitDesc xunit , YPathDesc yp) {
        return xunit.addYPath( yp);
    }

    public YPathDesc appendAttribute( YPathDesc yp, String attrName, String attrValue) {
        return yp.addAttribute(attrName, attrValue);
    }


    private void usage( String mess) throws UDFArgumentException {
        LOG.error("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);
        throw new UDFArgumentException("Invalid arguments. xunit_explode expects an array of structs containing dimension name and attribute values; " + mess);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objInspectorArr)
            throws UDFArgumentException {
        //// Need to make sure that it is our array of structs inspector
        if(objInspectorArr.length > 3 ) {
            usage(" Only one,two or three arguments");
        }
        ObjectInspector objInsp = objInspectorArr[0];
        if(objInsp.getCategory() != Category.LIST) {
            usage(" First arg must be a list");
        }

        if( objInspectorArr.length >1 ) {
            if( !(objInspectorArr[1] instanceof IntObjectInspector) ) {
                usage(" Number of dimensions must be a constant integer.");
            }
            maxDimInspector = (IntObjectInspector) objInspectorArr[1];
        }

        listInspector = (ListObjectInspector) objInsp;
        if(listInspector.getListElementObjectInspector().getCategory() != Category.STRUCT ) {
            usage(" Must be an array of structs");
        }
        structInspector = (StructObjectInspector) listInspector.getListElementObjectInspector();
        dimField = structInspector.getStructFieldRef("dim");
        if(dimField == null) {
            usage("Struct must have a 'dim' field");
        }
        if(dimField.getFieldObjectInspector().getCategory() != Category.PRIMITIVE
                || ((PrimitiveObjectInspector)dimField.getFieldObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            usage("dim field must be a string");
        }
        StringObjectInspector dimInspector = (StringObjectInspector) dimField.getFieldObjectInspector();

        attrNamesField = structInspector.getStructFieldRef("attr_names");
        if(attrNamesField == null) {
            usage("Struct must have a 'attr_names' field");
        }
        if(attrNamesField.getFieldObjectInspector().getCategory() != Category.LIST) {
            usage("attr_names needs to be a list");
        }
        attrNamesInspector = (ListObjectInspector) attrNamesField.getFieldObjectInspector();
        if(attrNamesInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
                || ((PrimitiveObjectInspector)attrNamesInspector.getListElementObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            usage("attr_names needs to be a list of strings");
        }
        attrNameInspector = (StringObjectInspector) attrNamesInspector.getListElementObjectInspector();

        attrValuesField = structInspector.getStructFieldRef("attr_values");
        if(attrValuesField == null) {
            usage("Struct must have a 'attr_values' field");
        }
        if(attrValuesField.getFieldObjectInspector().getCategory() != Category.LIST) {
            usage("attr_values needs to be a list");
        }
        attrValuesInspector = (ListObjectInspector) attrValuesField.getFieldObjectInspector();
        if(attrValuesInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE
                || ((PrimitiveObjectInspector)attrValuesInspector.getListElementObjectInspector()).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            usage("attr_values needs to be a list of strings");
        }
        attrValueInspector = (StringObjectInspector) attrValuesInspector.getListElementObjectInspector();


        if(objInspectorArr.length > 2) {
            if(objInspectorArr[2].getCategory() != Category.PRIMITIVE
                    || (((PrimitiveObjectInspector)objInspectorArr[2]).getPrimitiveCategory() != PrimitiveCategory.BOOLEAN)) {
                usage(" Explode Global flag must be a boolean");
            }
            globalFlagInspector = (BooleanObjectInspector) objInspectorArr[2];
        }

        /// We return a struct with one field, 'xunit'

        ArrayList<String> fieldNames = new ArrayList<String>();
        fieldNames.add("xunit");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add( PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<Object>  dimValuesList = (List<Object>) listInspector.getList(args[0]);
        boolean eventProcess=true;
        if( globalFlagInspector != null ) {
            boolean globalFlag = globalFlagInspector.get( args[2]);
            if(globalFlag) {
                eventProcess=false;
                forwardXUnit(GLOBAL_UNIT );
            }
        } else {
            forwardXUnit(GLOBAL_UNIT );
        }

        if(maxDimInspector != null) {
            maxDims = maxDimInspector.get( args[1]);
        }
        if(eventProcess) {
            try {
                if (dimValuesList != null && dimValuesList.size() > 0) {
                    //Check if first dim is event
                    Object firstStruct = dimValuesList.get(0);
                    List<YPathDesc> eventYPathDesc = generateYPaths(firstStruct);
                    for (YPathDesc ypath : eventYPathDesc) {
                        String dimName = ypath.getDimName();
                        if (!dimName.equalsIgnoreCase("event"))
                            return;
                    }
                    Object secondStruct = dimValuesList.get(1);
                    List<YPathDesc> spamYpathDesc = generateYPaths(secondStruct);
                    boolean nonSpammer = isNonSpammer(secondStruct);

                    List<XUnitDesc> xunitDescList = null;
                    if (nonSpammer) {
                        xunitDescList = getXUnitsForNonSpammers(eventYPathDesc, spamYpathDesc,
                                dimValuesList.subList(2, dimValuesList.size()));
                    } else {
                        xunitDescList = getXUnitsForSpammers(eventYPathDesc, spamYpathDesc);
                    }

                    for (XUnitDesc xunit : xunitDescList) {
                        forwardXUnit(xunit.toString());
                    }
                    incrCounter("NumXUnits", xunitDescList.size());
                }
            } catch (IllegalArgumentException illArg) {
                LOG.error("Error generating XUnits", illArg);
            }
        }
        else {
            //This handles the dau counts
            try{
                if (dimValuesList != null && dimValuesList.size() > 0) {
                    List<XUnitDesc> xunitDescList = null;
                    //The first struct object is a spam*, then just return all combinations upto dimsize=2
                    Object firstStruct = dimValuesList.get(0);
                    boolean nonSpammer = isNonSpammer(firstStruct);
                    if(nonSpammer) {
                        xunitDescList = getNonSpamXUnitsForDau(dimValuesList);
                    }else {
                        xunitDescList = getSpamXUnitsForDAU(dimValuesList);
                    }

                    for (XUnitDesc xunit : xunitDescList) {
                        forwardXUnit(xunit.toString());
                    }
                    incrCounter("NumXUnits", xunitDescList.size());
                }
            }catch(IllegalArgumentException illArg) {
                LOG.error("Error generating XUnits", illArg);
            }
        }
    }

    private List<XUnitDesc> getNonSpamXUnitsForDau(List<Object> dimObjectList) {

        List<XUnitDesc> results = new ArrayList<XUnitDesc>();
        Object firstStruct = dimObjectList.get(0);
        List<YPathDesc> spamYPath = generateYPaths(firstStruct);
        for(YPathDesc ypath:spamYPath) {
            XUnitDesc x1 = fromPath(ypath);
            results.add(x1);
            for(int i = 1; i < dimObjectList.size(); i++) {
                List<YPathDesc> ypaths1 = generateYPaths(i);
                for(YPathDesc y1 : ypaths1) {
                    XUnitDesc x2 = x1.addYPath(y1);
                    results.add(x2);
                    for(int j=i+1; j<dimObjectList.size(); j++) {
                        List<YPathDesc> ypaths2 = generateYPaths(j);
                        for(YPathDesc y2 : ypaths2){
                            XUnitDesc x3 = x2.addYPath(y2);
                            results.add(x3);
                            for(int k=j+1; k < dimObjectList.size(); k++) {
                                List<YPathDesc> ypaths3 = generateYPaths(k);
                                for(YPathDesc y3:ypaths3){
                                    XUnitDesc x4 = x3.addYPath(y3);
                                    results.add(x4);
                                }
                            }
                        }
                    }
                }
            }
        }

        for(int i=1; i < dimObjectList.size(); i++) {
            List<YPathDesc> ypaths = generateYPaths(dimObjectList.get(i));
            XUnitDesc xunit = fromPath(ypaths.get(0));
            results.add(xunit);
            for(int j=1;j<ypaths.size();j++){
                XUnitDesc x2 = xunit.addYPath(ypaths.get(j));
                results.add(x2);
            }
        }

        return results;
    }

    private List<XUnitDesc> getSpamXUnitsForDAU(List<Object> remDimObjects) {

        List<XUnitDesc> results = new ArrayList<XUnitDesc>();
        for(int i = 0; i < remDimObjects.size(); i++){
            List<YPathDesc> ypathList = generateYPaths(remDimObjects.get(i));
            for(YPathDesc ypath:ypathList){
                XUnitDesc x1 = fromPath(ypath);
                //x1.addYPath(ypath);
                results.add(x1);
                for(int j=i+1;j<remDimObjects.size();j++){
                    List<YPathDesc> ypathDescList2 = generateYPaths(remDimObjects.get(j));
                    for(YPathDesc ypath2 : ypathDescList2) {
                        XUnitDesc x2 = x1.addYPath(ypath2);
                        //x2.addYPath(ypath2);
                        results.add(x2);
                    }
                }
            }

        }
        return results;
    }

    private boolean isNonSpammer(Object spamStruct) {
        List<YPathDesc> spamYpathDesc = generateYPaths(spamStruct);
        boolean nonSpammer = false;
        for (YPathDesc ypath : spamYpathDesc) {
            String dimName = ypath.getDimName();
            if (dimName.equalsIgnoreCase("spam")) {
                String attrVal = ypath.getAttrValues()[0];
                if (attrVal.equalsIgnoreCase("nonspammer-validated"))
                    nonSpammer = true;
            }
        }
        return nonSpammer;
    }

    public List<XUnitDesc> getXUnitsForSpammers(List<YPathDesc> eventsYPaths, List<YPathDesc> spamYPaths) {
        List<XUnitDesc> results = new ArrayList<XUnitDesc>();
        for (YPathDesc yPath : eventsYPaths) {
            XUnitDesc xUnitDesc = fromPath(yPath);
            results.add(xUnitDesc);
            for (YPathDesc spamYpath : spamYPaths)
                results.add(xUnitDesc.addYPath(spamYpath));
        }
        return results;
    }

    public List<XUnitDesc> getXUnitsForNonSpammers(List<YPathDesc> eventsYPaths, List<YPathDesc> spamYPaths,
                                                   List<Object> remDimObjectList){
        List<XUnitDesc> results = new ArrayList<XUnitDesc>();
        List<XUnitDesc> twoLevel = new ArrayList<XUnitDesc>();
        List<XUnitDesc> threeLevel = new ArrayList<XUnitDesc>();
        List<XUnitDesc> fourLevelXUnits = new ArrayList<XUnitDesc>();
        //first create the event xunit
        for(YPathDesc eventYPath : eventsYPaths) {
            XUnitDesc xunitDesc = fromPath(eventYPath);
            results.add(xunitDesc);
            for(YPathDesc spamYpath : spamYPaths) {
                results.add(xunitDesc.addYPath(spamYpath));
                twoLevel.add(xunitDesc.addYPath(spamYpath));
            }
        }
        int resultSize = results.size();
        int itemsToSkip=0;
        boolean outerBreak=false;
        boolean validCustomSeg = false;
        //Check if there's any valid custom segments
        List<XUnitDesc> customXUnits = new ArrayList<XUnitDesc>();
        for(int i = 0; i < remDimObjectList.size(); i++) {
            Object firstStruct = remDimObjectList.get(i);
            List<YPathDesc> ypathCustom = generateYPaths(firstStruct);
            if(ypathCustom!=null && ypathCustom.size() > 0)
                //customXUnits.add(new XUnitDesc(ypathCustom));
                for (YPathDesc ypath : ypathCustom) {
                    String dimName = ypath.getDimName();
                    String dimVal = ypath.getAttrNames()[0];
                    if(!dimVal.equalsIgnoreCase("null") && dimName.equalsIgnoreCase("custom")){
                        itemsToSkip++;
                        validCustomSeg = true;
                        customXUnits.add(fromPath(ypath));
                        //for(XUnitDesc xunit : results){
                        for(int x=1;x < resultSize;x++){
                            XUnitDesc xunit = results.get(x);
                            XUnitDesc x1 = xunit.addYPath(ypath);
                            //results.add(x1);
                            if(x1.numDims()==4)
                                threeLevel.add(x1);
                            else if(x1.numDims() <= 3)
                                twoLevel.add(x1);
                            else
                                fourLevelXUnits.add(x1);
                        }
                    }else {
                        outerBreak=true;
                        break;
                    }
                }
            if(outerBreak)
                break;
        }
        if(validCustomSeg)
            maxDims++;
        if(customXUnits.size() == 2){
            //create a new xunit with a combination of both of them
            XUnitDesc first = customXUnits.get(0);
            XUnitDesc merged = first.addYPath(customXUnits.get(1).getYPathDesc()[0]);
            for(int i=1; i<resultSize;i++){
                //XUnitDesc xunit = results.get(i);
                XUnitDesc x1=results.get(i);
                for(YPathDesc ypath:merged.getYPathDesc()) {
                    x1 = x1.addYPath(ypath);
                }
                //results.add(x1);
                if(x1.numDims()==4)
                    threeLevel.add(x1);
                else if(x1.numDims() <= 3)
                    twoLevel.add(x1);
                else
                    fourLevelXUnits.add(x1);
            }
        }

        if(customXUnits.size() ==3) {
            for(int i=0;i<customXUnits.size();i++){
                XUnitDesc prev = customXUnits.get(i);
                for(int j=i+1;j<customXUnits.size(); j++){
                    XUnitDesc next = customXUnits.get(j);
                    XUnitDesc merged = prev.addYPath(next.getYPathDesc()[0]);
                    for(int x=1;x<resultSize;x++){
                        XUnitDesc x1 = results.get(x);
                        for(YPathDesc ypath:merged.getYPathDesc()){
                            x1=x1.addYPath(ypath);
                        }
                        //XUnitDesc x1 = xunit.addYPath(merged.getYPathDesc()[0]);
                        //results.add(x1);
                        if(x1.numDims()==4)
                            threeLevel.add(x1);
                        else if(x1.numDims() <= 3)
                            twoLevel.add(x1);
                        else
                            fourLevelXUnits.add(x1);
                    }

                }
            }
            // XUnitDesc allXUnit = customXUnits.get(0).addYPath(customXUnits.get(1).getYPathDesc()[0]).addYPath(customXUnits.get(2).getYPathDesc()[2]);
            List<YPathDesc> yPathDescs = new ArrayList<YPathDesc>();
            for(XUnitDesc xunit : customXUnits){
                for(YPathDesc ypath:xunit.getYPathDesc())
                    yPathDescs.add(ypath);
            }
            for(int x=1;x<resultSize;x++){
                XUnitDesc x1 = results.get(x);
                for(YPathDesc ypath:yPathDescs){
                    x1=x1.addYPath(ypath);
                }
                // x1 = xunit.addYPath(allXUnit.getYPathDesc()[0]);
                //results.add(x1);
                if(x1.numDims()==4)
                    threeLevel.add(x1);
                else if(x1.numDims() <= 3)
                    twoLevel.add(x1);
                else
                    fourLevelXUnits.add(x1);
            }
        }

        List<XUnitDesc> twoLevelResults = getFinalXUnits(twoLevel,remDimObjectList.subList(3,remDimObjectList.size()));
        List<XUnitDesc> threeLevelResults =
                addRemainingDimXunits(threeLevel,remDimObjectList.subList(3,remDimObjectList.size()));
        List<XUnitDesc> finalResult = new ArrayList<XUnitDesc>();
        finalResult.add(results.get(0));
        finalResult.addAll(twoLevelResults);
        finalResult.addAll(threeLevelResults);
        finalResult.addAll(fourLevelXUnits);
        return finalResult;
    }

    public List<XUnitDesc> addRemainingDimXunits(List<XUnitDesc> xunitResult1, List<Object> remDimObjects) {
        List<XUnitDesc> results = new ArrayList<XUnitDesc>();

        for(int x = 0; x < xunitResult1.size() ; x++) {
            XUnitDesc xunit = xunitResult1.get(x);
            for(int i=0;i<remDimObjects.size();i++){
                List<YPathDesc> ypathList = generateYPaths(remDimObjects.get(i));
                for(YPathDesc ypath:ypathList) {
                    XUnitDesc x1 = xunit.addYPath(ypath);
                    //x1.addYPath(ypath);
                    if(x1.numDims() <= maxDims)
                        results.add(x1);
                }
            }
        }
        results.addAll(xunitResult1);
        return results;
    }

    public List<XUnitDesc> getFinalXUnits(List<XUnitDesc> xunitResult1, List<Object> remDimObjects) {
        List<XUnitDesc> results = new ArrayList<XUnitDesc>();

        //for(XUnitDesc xunit : xunitResult1) {
        for(int x = 0; x < xunitResult1.size(); x++){
            XUnitDesc xunit = xunitResult1.get(x);
            for(int i = 0; i < remDimObjects.size(); i++){
                List<YPathDesc> ypathList = generateYPaths(remDimObjects.get(i));
                for(YPathDesc ypath:ypathList){
                    XUnitDesc x1 = xunit.addYPath(ypath);
                    //x1.addYPath(ypath);
                    results.add(x1);
                    for(int j=i+1;j<remDimObjects.size();j++){
                        List<YPathDesc> ypathDescList2 = generateYPaths(remDimObjects.get(j));
                        for(YPathDesc ypath2 : ypathDescList2) {
                            XUnitDesc x2 = x1.addYPath(ypath2);
                            //x2.addYPath(ypath2);
                            if(x2.numDims() <= maxDims)
                                results.add(x2);
                        }
                    }
                }

            }
        }

        results.addAll(xunitResult1);

        return results;
    }

    private void forwardXUnit( String xunit) throws HiveException {
        ///LOG.info(" Forwarding XUnit " + xunit);
        xunitFieldArr[0] = xunit;
        forward( xunitFieldArr);
    }


    /**
     *  Generate the "per-dimension" portion of the xunit
     *
     */
    private YPathDesc getDimBase( Object structObj) {
        return new YPathDesc( structInspector.getStructFieldData(structObj,dimField).toString() );
    }

    /**
     *  Clean out any special characters in the string
     */
    private String clean( String attrVal) {
        String trimVal = attrVal.trim();
        if(trimVal.length() == 0) {
            return null;
        }
        if(trimVal.toLowerCase().equals("null")) {
            return null;
        }
        String noSlash = trimVal.replace( '/',' ');

        return noSlash;
    }

    private List<YPathDesc> generateYPaths( Object structObj) throws IllegalArgumentException {
        List nameList = (List) structInspector.getStructFieldData(structObj, attrNamesField);
        List valueList = (List) structInspector.getStructFieldData(structObj, attrValuesField);

        List<YPathDesc> retVal = new ArrayList<YPathDesc>();
        if( nameList == null || valueList == null) {
            return retVal;
        }

        if(nameList.size() != valueList.size()) {
            throw new IllegalArgumentException("Number of atttribute names must equal number of attribute values");
        }

        List<YPathDesc> prevYPaths = new ArrayList<YPathDesc>();
        List<YPathDesc> nextPrevYPaths = new ArrayList<YPathDesc>();

        prevYPaths.add( getDimBase(structObj) );
        for(int i=0; i< nameList.size(); ++i) {
            String attrValue = attrValueInspector.getPrimitiveJavaObject(valueList.get(i));
            String attrName = attrNameInspector.getPrimitiveJavaObject(nameList.get(i));

            if(attrValue != null) {
                if(! attrValue.contains("|")) {
                    String cleanVal = clean( attrValue);
                    if(cleanVal != null) {
                        for(YPathDesc prevYPath : prevYPaths) {
                            YPathDesc newYp = prevYPath.addAttribute(attrName, cleanVal);
                            retVal.add(newYp);
                            nextPrevYPaths.add(newYp);
                        }
                    }
                } else{
                    ///// If we want to emit multiple rows for an xunit, for a particular YPath
                    ////  (ie. Both Asian and Hispanic ethnicity  )
                    //// Assumption is that multiple values will be |-pipe separated ..
                    String[] subVals = attrValue.split("\\|");
                    for(String subVal : subVals) {
                        String cleanSubVal = clean( subVal);
                        if( cleanSubVal != null) {
                            for(YPathDesc prevYPath : prevYPaths) {
                                YPathDesc newYp = prevYPath.addAttribute(attrName, cleanSubVal);
                                retVal.add(newYp);
                                nextPrevYPaths.add( newYp);
                            }
                        }
                    }
                }
            }
            prevYPaths = nextPrevYPaths;
            nextPrevYPaths = new ArrayList<YPathDesc>();
        }
        return retVal;
    }

}
