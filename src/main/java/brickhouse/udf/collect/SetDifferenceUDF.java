package brickhouse.udf.collect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
 *   UDF for the set difference of two arrays or maps ...
 * @author jeromebanks
 *
 */

@Description(name="set_diff",
value = "_FUNC_(a,b) - Returns a list of those items in a, but not in b " 
)
public class SetDifferenceUDF extends GenericUDF {
	private Category category;
	private ListObjectInspector listInspector;
	private MapObjectInspector mapInspector;

	public List evaluate( List l1, List l2 ) {
		    if( l1 == null ) {
		    	return new ArrayList();
		    }
		    //// Use a HashSet to avoid linear lookups , for large lists 
			HashSet negSet = new HashSet();
			if(l2 != null) {
			  negSet.addAll( l2);
			} else {
				return l1;
			}
			ArrayList newList = new ArrayList();
			for( Object obj: l1) {
				if( ! negSet.contains( obj)) {
					newList.add( obj);
				}
			}
			return newList;
	}
	
	public Map evaluate( Map m1, Map m2) {
		HashMap newMap = new HashMap();
		if( m1 != null && m1.size() > 0)
			for(Object k : m1.keySet()) {
				if( m2 != null ) {
					if( !m2.containsKey(k)) {
						newMap.put(k,  m1.get(k));
					}
				} else {
				    newMap.put( k, m1.get(k));
				}
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
			throw new UDFArgumentException("Usage: set_diff takes 2  maps or lists, and returns the difference");
		}
		ObjectInspector first = ObjectInspectorUtils.getStandardObjectInspector(args[0] );
		
		if(first.getCategory() == Category.LIST) {
			category = first.getCategory();
			listInspector = (ListObjectInspector)first;
		} else if( first.getCategory() == Category.MAP) {
			category = first.getCategory();
			mapInspector = (MapObjectInspector)first;
		} else {
			throw new UDFArgumentException(" set_diff only takes maps or lists.");
		}
		//// Check that the type in it is all the same type ..
		//// Check that the are all the same type ...
		for(int i=1; i<args.length; ++i) {
			ObjectInspector argInsp = args[i];
			if(argInsp.getCategory() != category) {
				throw new UDFArgumentException("set_diff must either be all maps or all lists");
			}
		}
		return first;
	}
	
}
