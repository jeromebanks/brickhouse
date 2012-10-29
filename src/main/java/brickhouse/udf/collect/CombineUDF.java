package brickhouse.udf.collect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 *   UDF for combining two lists or two maps together
 * @author jeromebanks
 *
 */

@Description(name="combine",
value = "_FUNC_(a,b) - Returns a combined list of two lists, or a combined map of two maps " 
)
public class CombineUDF extends GenericUDF {
	private Category category;
	private ListObjectInspector listInspector;
	private MapObjectInspector mapInspector;

	public List evaluate( List l1, List l2 ) {
		ArrayList newList = new ArrayList();
		if(l1 != null && l1.size() > 0)
			newList.addAll( l1);
		
		if(l2 != null && l2.size() > 0)
			newList.addAll( l2);
		
		return newList;
	}
	
	public Map evaluate( Map m1, Map m2) {
		HashMap newMap = new HashMap();
		if( m1 != null && m1.size() > 0)
			for(Object k : m1.keySet()) {
				newMap.put( k, m1.get(k));
			}
		 
		if( m2 != null && m2.size() > 0)
			for(Object k : m2.keySet()) {
				newMap.put( k, m2.get(k));
			}
		return newMap;
	}

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		if( category == Category.LIST) {
			List theList = listInspector.getList( args[0].get());
			for(int i=1;  i<args.length;++i) {
				theList = evaluate(theList, listInspector.getList(args[i].get()));
			}
			return theList;
		} else if( category == Category.MAP) {
			Map theMap = mapInspector.getMap( args[0].get());
			for(int i=1;  i<args.length;++i) {
				theMap = evaluate(theMap, mapInspector.getMap(args[i].get()));
			}
			return theMap;
		} else {
			throw new HiveException(" Only maps or lists are supported ");
		}
	}

	@Override
	public String getDisplayString(String[] args) {
		StringBuilder sb = new StringBuilder("combine( ");
		for( int i=0; i<args.length -1; ++i) {
			sb.append(args[i]);
			sb.append( ",");
		}
		sb.append( args[args.length -1]);
		sb.append(")");
		return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		if( args.length < 2) {
			throw new UDFArgumentException("Usage: combine takes 2 or more maps or lists, and combines the result");
		}
		ObjectInspector first = ObjectInspectorUtils.getStandardObjectInspector(args[0] );
		
		if(first.getCategory() == Category.LIST) {
			category = first.getCategory();
			listInspector = (ListObjectInspector)first;
		} else if( first.getCategory() == Category.MAP) {
			category = first.getCategory();
			mapInspector = (MapObjectInspector)first;
		} else {
			throw new UDFArgumentException(" combine only takes maps or lists.");
		}
		//// Check that the type in it is all the same type ..
		//// Check that the are all the same type ...
		for(int i=1; i<args.length; ++i) {
			ObjectInspector argInsp = args[i];
			if(argInsp.getCategory() != category) {
				throw new UDFArgumentException("Combine must either be all maps or all lists");
			}
		}
		return first;
	}
	
}
