package brickhouse.analytics.uniques;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SketchSet implements ICountDistinct {
    public  static int DEFAULT_MAX_ITEMS = 5000;
    private  int maxItems = DEFAULT_MAX_ITEMS;
	private TreeMap<Long,String> sortedMap;
	private HashFunction hash;
	
    
    public SketchSet() {
    	sortedMap = new TreeMap<Long,String>();
    	hash = Hashing.md5();
    }
    	
    
    public SketchSet(int max ) {
    	this.maxItems = max;
    	sortedMap = new TreeMap<Long,String>();
    	hash = Hashing.md5();
    }
    
	public void addHashItem( long hash, String str) {
		if(sortedMap.size() < maxItems) {
			sortedMap.put( hash, str);
		} else {
			long maxHash = sortedMap.lastKey();
			if( hash< maxHash) {
				sortedMap.remove(maxHash);
				sortedMap.put( hash,str);
			}
		}
	}
	/**
	 *   for testing 
	 * @param hash
	 */
	public void addHash( long hash) {
		addHashItem( hash, Long.toString( hash));
	}
	
	public void addItem( String str) {
		HashCode hc = hash.hashString( str);
		this.addHashItem( hc.asLong(), str);
	}
	
	public List<String> getMinHashItems() {
	  return new ArrayList(this.sortedMap.values());
	}
	
	public SortedMap<Long,String> getHashItemMap() {
		return this.sortedMap;
	}
	
	public void clear() {
		this.sortedMap.clear();
	}
	
	public int getMaxItems() {
		return maxItems;
	}
	
	public double estimateReach() {
		if(sortedMap.size() < maxItems) {
			return sortedMap.size();
		}
		long maxHash = sortedMap.lastKey();
		///BigInteger maxBig = BigInteger.valueOf(maxHash).add(MAX_LONG);
		double ratio = ((double)maxItems)/((double)(maxHash + Long.MAX_VALUE));
		
		double est = ratio*((double)Long.MAX_VALUE)*2.0;
		
		return est;
	}
	
	
	public void combine( SketchSet other) {
		for( Entry<Long,String> entry: other.sortedMap.entrySet() ) {
			addHashItem( entry.getKey(), entry.getValue());
		}
	}
	
}