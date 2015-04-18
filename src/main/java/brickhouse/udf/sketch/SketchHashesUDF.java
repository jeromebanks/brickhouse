package brickhouse.udf.sketch;
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
 **/

import brickhouse.analytics.uniques.SketchSet;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import java.util.List;

/*
 * Return the set of MD5 hashes from a sketch set,
 *    
 */
@Description(name = "sketch_hashes",
        value = "_FUNC_(x) - Return the MD5 hashes associated with a KMV sketch set of strings "
)
public class SketchHashesUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(SketchHashesUDF.class);
    private ListObjectInspector listInspector;
    private StringObjectInspector listElemInspector;
    private StandardListObjectInspector retInspector;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object obj = arg0[0].get();
        if (obj == null) {
            return null;
        }
        List oldList = listInspector.getList(obj);
        int sketchSize = listInspector.getListLength(obj);
        SketchSet sketchSet = new SketchSet(sketchSize);
        for (Object oldObj : oldList) {
            if (oldObj == null) {
                LOG.warn(" Object in uninspected List is null");
            } else {
                String newStr = listElemInspector.getPrimitiveJavaObject(oldObj);
                if (newStr == null)
                    LOG.warn(" inspected object is null !!! ");
                else
                    sketchSet.addItem(newStr);

            }
        }
        return sketchSet.getMinHashes();
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "sketch_hashes(" + arg0[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException("sketch_hashes takes a list of strings ");
        }
        listInspector = (ListObjectInspector) arg0[0];
        if (!(listInspector.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("sketch_hashes takes a list of strings ");
        }
        listElemInspector = (StringObjectInspector) listInspector.getListElementObjectInspector();

        retInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return retInspector;
    }
}
