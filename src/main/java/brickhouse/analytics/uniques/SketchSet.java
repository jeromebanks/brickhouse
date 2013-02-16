package brickhouse.analytics.uniques;
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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
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
    		Long hashLong = hash;
    		if(! sortedMap.containsKey( hashLong)) {
    			long maxHash = sortedMap.lastKey();
    			if( hash< maxHash) {
    				sortedMap.remove(maxHash);
    				sortedMap.put( hashLong,str);
    			}
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
	
	public long lastHash() {
		return sortedMap.lastKey();
	}
	
	public double estimateReach() {
		if(sortedMap.size() < maxItems) {
			return sortedMap.size();
		}
		long maxHash = sortedMap.lastKey();
		///double maxHashShifted = (double)( (double)maxHash + (double)Long.MAX_VALUE );
		
		///double ratio = ((double)maxItems)/maxHashShifted;
		///double est = ratio*((double)Long.MAX_VALUE)*2.0;
		///return est;
		
		BigDecimal maxHashShifted = new BigDecimal(BigInteger.valueOf( maxHash).add( BigInteger.valueOf( Long.MAX_VALUE)));
	
		BigDecimal bigMaxItems = new BigDecimal( maxItems*2).multiply( BigDecimal.valueOf( Long.MAX_VALUE));
		BigDecimal ratio = bigMaxItems.divide(maxHashShifted, RoundingMode.HALF_EVEN);
		return ratio.doubleValue();
		
	}
	
	
	public void combine( SketchSet other) {
		for( Entry<Long,String> entry: other.sortedMap.entrySet() ) {
			addHashItem( entry.getKey(), entry.getValue());
		}
	}
	
}