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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * UDF for combining two lists or two maps together,
 * across multiple rows, ( in a grouping ),
 * so that state can be store, and we can calculate
 * things like  "previous actors"
 */

@Description(name = "combine_previous_sketch",
        value = "_FUNC_(grouping, map) - Returns a map of the combined keys of previous calls to this "
)
public class CombinePreviousSketchUDF extends GenericUDF {
    private StringObjectInspector groupInspector;
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private String lastGrouping = null;
    private SketchSet prevValue = new SketchSet();

    public List evaluate(List<String> l1, List<String> l2) {
        ArrayList newList = new ArrayList();
        if (l1 != null && l1.size() > 0)
            newList.addAll(l1);

        if (l2 != null && l2.size() > 0)
            newList.addAll(l2);

        return newList;
    }


    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        String grouping = this.groupInspector.getPrimitiveJavaObject(args[0].get());
        if (lastGrouping == null || !lastGrouping.equals(grouping)) {
            lastGrouping = grouping;
            prevValue = new SketchSet();
        }
        List<String> prevHashItems = prevValue.getMinHashItems();


        List newList = listInspector.getList(args[1].get());
        if (newList != null) {
            for (Object strObj : newList) {
                String str = ((StringObjectInspector) listInspector.getListElementObjectInspector()).getPrimitiveJavaObject(strObj);
                prevValue.addItem(str);
            }
        }
        return prevHashItems;
    }

    @Override
    public String getDisplayString(String[] args) {
        StringBuilder sb = new StringBuilder("combine_previous_sketch( ");
        for (int i = 0; i < args.length - 1; ++i) {
            sb.append(args[i]);
            sb.append(",");
        }
        sb.append(args[args.length - 1]);
        sb.append(")");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("Usage: combine_previous_sketch takes a grouping string, and a sketch_set");
        }
        ///ObjectInspector first = ObjectInspectorUtils.getStandardObjectInspector(args[0] );
        ObjectInspector first = args[0];
        if ((first.getCategory() != Category.PRIMITIVE)
                || ((PrimitiveObjectInspector) first).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage: combine_previous_sketch takes a grouping string, and a sketch_set");
        } else {
            groupInspector = (StringObjectInspector) first;
        }

        ///ObjectInspector second = ObjectInspectorUtils.getStandardObjectInspector(args[1] );
        ObjectInspector second = args[1];
        Category category = second.getCategory();
        if (category == Category.LIST) {
            listInspector = (ListObjectInspector) second;
        } else {
            throw new UDFArgumentException(" combine_previous_sketch only takes sketch_sets.");
        }


        ListObjectInspector sketchListInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return sketchListInspector;
    }

}
