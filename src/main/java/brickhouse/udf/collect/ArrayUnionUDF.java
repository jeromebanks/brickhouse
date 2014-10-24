package brickhouse.udf.collect;
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


import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.log4j.Logger;


/**
 * Return a list of unique entries, for a given set of lists.
 *
 * {1, 2} ∪ {1, 2} = {1, 2}
 * {1, 2} ∪ {2, 3} = {1, 2, 3}
 * {1, 2, 3} ∪ {3, 4, 5} = {1, 2, 3, 4, 5}
 */

@Description(name = "array_union",
		value = "_FUNC_(array1, array2, ...) - Returns the union of a set of arrays "
)
public class ArrayUnionUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(ArrayUnionUDF.class);
	private StandardListObjectInspector retValInspector;
	private ListObjectInspector[] listInspectorArr;


	private class InspectableObject implements Comparable {
		public Object o;
		public ObjectInspector oi;

		public InspectableObject(Object o, ObjectInspector oi) {
			this.o = o;
			this.oi = oi;
		}

		@Override
		public int hashCode() {
			return ObjectInspectorUtils.hashCode(o, oi);
		}

		@Override
		public int compareTo(Object arg0) {
			InspectableObject otherInsp = (InspectableObject) arg0;
			return ObjectInspectorUtils.compare(o, oi, otherInsp.o, otherInsp.oi);
		}

		@Override
		public boolean equals(Object other) {
			return compareTo(other) == 0;
		}
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {

		Set<InspectableObject> objects = new TreeSet<InspectableObject>();

		for (int i = 0; i < arg0.length; ++i) {
			Object undeferred = arg0[i].get();
			for (int j = 0; j < listInspectorArr[i].getListLength(undeferred); ++j) {
				Object nonStd = listInspectorArr[i].getListElement(undeferred, j);
				InspectableObject stdInsp = new InspectableObject(nonStd, listInspectorArr[i].getListElementObjectInspector());
				objects.add(stdInsp);
			}
		}

		List retVal = (List) retValInspector.create(0);
		for (Object io : objects) {
			InspectableObject inspObj = (InspectableObject) io;
			Object stdObj = ObjectInspectorUtils.copyToStandardObject(inspObj.o, inspObj.oi);
			retVal.add(stdObj);
		}
		return retVal;
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "array_union(" + arg0[0] + ", " + arg0[1] + " )";
	}


	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		if (arg0.length < 2) {
			throw new UDFArgumentException(" Expecting at least two arrays as arguments ");
		}
		ObjectInspector first = arg0[0];
		listInspectorArr = new ListObjectInspector[arg0.length];
		if (first.getCategory() == Category.LIST) {
			listInspectorArr[0] = (ListObjectInspector) first;
		} else {
			throw new UDFArgumentException(" Expecting an array as first argument ");
		}
		for (int i = 1; i < arg0.length; ++i) {
			if (arg0[i].getCategory() != Category.LIST) {
				throw new UDFArgumentException(" Expecting arrays arguments ");
			}
			ListObjectInspector checkInspector = (ListObjectInspector) arg0[i];
			if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector.getListElementObjectInspector())) {
				throw new UDFArgumentException(" Array types must match " + listInspectorArr[0].getTypeName() + " != " + checkInspector.getTypeName());
			}
			listInspectorArr[i] = checkInspector;
		}

		retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
		return retValInspector;
	}

}
