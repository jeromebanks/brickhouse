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


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

import java.util.ArrayList;


/**
 * Creates an array of constant values for passed in array size and constant value. Usage example:
 *  array_constants(3, 1234) would produce array result of: [1234, 1234, 1234]
 *
 */

public class ArrayOfConstantsUDF extends GenericUDF {
	private IntObjectInspector lengthOI;
    private PrimitiveObjectInspector constantOI;
    private StandardListObjectInspector returnOI;

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
        int length = lengthOI.get( args[0].get() );
        Object constantValue = ObjectInspectorUtils.copyToStandardObject(args[1].get(), constantOI);
        ArrayList<Object> constantList = new ArrayList<Object>(length);
        for(int i=0; i < length; i++) {
            constantList.add(i, constantValue);
        }

        return constantList;
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "array_constants(" + arg0[0] + ", " + arg0[1] + " )";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		ObjectInspector first = args[0];
        if(first instanceof IntObjectInspector)  {
            lengthOI = (IntObjectInspector) args[0];
        } else {
            throw new UDFArgumentException(" Expecting one int and one primitive value for constant as arguments ");
        }

	    ObjectInspector second = args[1];
	    if( second.getCategory() == Category.PRIMITIVE) {
	    	constantOI = (PrimitiveObjectInspector) second;
	    } else {
			throw new UDFArgumentException(" Expecting one int and one primitive value for constant as arguments ");
	    }
	    
	    returnOI = ObjectInspectorFactory.getStandardListObjectInspector(constantOI);
		return returnOI;
	}
}
