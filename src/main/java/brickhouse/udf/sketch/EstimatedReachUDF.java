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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

/**
 * Interpret a list of strings as a sketch_set
 * and return an estimated reach number
 */
@Description(name = "estimated_reach",
        value = "_FUNC_(x) - Estimate reach from a  sketch set of Strings. "
)
public class EstimatedReachUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(EstimatedReachUDF.class);

    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector elemInspector;
    private PrimitiveCategory elemCategory;
    private IntObjectInspector lengthInspector;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Object listObj = arg0[0].get();
        int maxItems = SketchSet.DEFAULT_MAX_ITEMS;
        if (arg0.length > 1) {
            maxItems = lengthInspector.get(arg0[1].get());
        }

        int listLen = listInspector.getListLength(listObj);
        if (listLen < maxItems) {
            return (long) listLen;
        }
        if (listLen > maxItems) {
            LOG.warn("estimated_reach: List length " + listLen + " is greater than sketch set Max items " + maxItems);
        }
        Object uninspMax = listInspector.getListElement(listObj, maxItems - 1);
        switch (this.elemCategory) {
            case STRING:
                StringObjectInspector strInspector = (StringObjectInspector) elemInspector;
                String lastItem = strInspector.getPrimitiveJavaObject(uninspMax);
                double reach = SketchSet.EstimatedReach(lastItem, maxItems);
                if (reach > listLen)
                    return (long) (reach);
                else
                    return (long) listLen;
            case LONG:
                LongObjectInspector longInspector = (LongObjectInspector) elemInspector;
                long lastHash = longInspector.get(uninspMax);
                double reachHash = SketchSet.EstimatedReach(lastHash, maxItems);
                if (reachHash > listLen)
                    return (long) (reachHash);
                else
                    return (long) listLen;
            default:
                /// should not happen
                throw new HiveException("Unexpected category type");
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("estimated_reach( ");
        for (int i = 0; i < arg0.length - 1; ++i) {
            sb.append(arg0[i]);
            sb.append(" , ");
        }
        sb.append(arg0[arg0.length - 1]);
        sb.append(" )");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 1 && arg0.length != 2) {
            throw new UDFArgumentException("estimated_reach takes an array of strings or an array of hashes, and an optional sketch size");
        }
        if (arg0[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException("estimated_reach takes an array of strings or an array of hashes, and an optional sketch size");
        }
        this.listInspector = (ListObjectInspector) arg0[0];
        if (listInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("estimated_reach takes an array of strings or an array of hashes, and an optional sketch size");
        }
        this.elemInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
        LOG.info(" Element category is " + this.elemInspector.getCategory());
        this.elemCategory = this.elemInspector.getPrimitiveCategory();
        if (this.elemCategory != PrimitiveCategory.STRING
                && this.elemCategory != PrimitiveCategory.LONG) {
            throw new UDFArgumentException("estimated_reach takes an array of strings or an array of hashes, and an optional sketch size");
        }
        if (arg0.length > 1) {
            if (!(arg0[1] instanceof IntObjectInspector)) {
                throw new UDFArgumentException("estimated_reach takes an array of strings or an array of hashes, and an optional sketch size");

            }
            this.lengthInspector = (IntObjectInspector) arg0[1];
        }

        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }


}
