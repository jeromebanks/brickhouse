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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SketchSet implements ICountDistinct {
	static final int SIZEOF_LONG = 64;
	
    public  static int DEFAULT_MAX_ITEMS = 5000;
    private  int maxItems = DEFAULT_MAX_ITEMS;
	private TreeMap<BigInteger,String> sortedMap;
	private static HashFunction HASH = Hashing.md5();
	///private static HashFunction HASH = Hashing.md5();
	///private static HashFunction HASH = Hashing.sha1();

	private int hashLength = 16;
	
    
    public SketchSet() {
    	sortedMap = new TreeMap<BigInteger,String>();
    }
    	
    
    public SketchSet(int max ) {
    	this.maxItems = max;
    	sortedMap = new TreeMap<BigInteger,String>();
    }
    
    public SketchSet(int max , int hashLength) {
    	this.maxItems = max;
    	sortedMap = new TreeMap<BigInteger,String>();
    	this.hashLength = hashLength;
    }

    public void addHashItem( long hash, String str) {
       addHashItem( LongToByteArr(hash), str);
    }

    public void addHashItem( byte[] hash, String str) {
       addHashItem( new BigInteger(hash), str);
    }

    public void addHashItem( BigInteger hash, String str) {
    	///System.out.println(" HASH IS " + hash.toString() );
    	if(sortedMap.size() < maxItems) {
    		sortedMap.put( (hash), str);
    	} else {
    		if(! sortedMap.containsKey( hash)) {
    			BigInteger maxHash = sortedMap.lastKey();
    			///System.out.println(" NEW HASH is " + hash + " OLD HASH IS " + maxHash );
    			if( maxHash.compareTo(hash) > 0 ) {
    			///System.out.println(" REPLACING NEW HASH is " + hash + " OLD HASH IS " + maxHash );
    				sortedMap.remove(maxHash);
    				sortedMap.put( hash,str);
    			}
    		}
    	}
    }

	/**
	 *   for testing 
	 * @param hash
	 */
	public void addHash( byte[] hash) {
		addHash( new BigInteger(hash));
	}
	public void addHash(BigInteger hash) {
		addHashItem( hash, hash.toString());
	}
	
	public void addHash( long hashLong) {
		byte[] hash = LongToByteArr( hashLong);
		addHash( hash);
	}

	public void addItem( String str) {
		///HashCode hc = HASH.hashUnencodedChars( str);
		HashCode hc = HASH.hashString( str);
		///this.addHashItem( hc.asLong(), str);
		///System.out.println(" HASH OF " + str + " is " + hc.asLong()  + " BIG INT + = " + ( new BigInteger(hc.asBytes())));
		this.addHashItem( hc.asBytes(), str);
	}
	
	public List<String> getMinHashItems() {
	  return new ArrayList(this.sortedMap.values());
	}
	
	public SortedMap<BigInteger,String> getHashItemMap() {
		return this.sortedMap;
	}
	
	public List<BigInteger> getMinHashes() {
	   return new ArrayList( this.sortedMap.keySet());
	}
	
	public void clear() {
		this.sortedMap.clear();
	}
	
	public int getMaxItems() {
		return maxItems;
	}
	
	public BigInteger lastHash() {
		return sortedMap.lastKey();
	}
	
	public String lastItem() {
		return sortedMap.lastEntry().getValue();
	}
	
	public double estimateReach() {
		if(sortedMap.size() < maxItems) {
			return sortedMap.size();
		}
		BigInteger maxHash = sortedMap.lastKey();
		return EstimatedReach(maxHash, maxItems, hashLength);
	}
	
	
	/** 
	 *  Converts a Byte array to a Long
	 *   Cuts off bytes beyond 8 
	 * @param byteArr
	 * @return
	 */
	static public long ByteArrToLong( byte[] byteArr) {
		long val = 0l;
		for(int i=0; i<8; ++i) {
		  val = val | (byteArr[i] << i*8);
		}
		return val;
	}
	static public byte[] LongToByteArr( long lng) {
		byte[] byteVal = new byte[8];
		for(int i=0; i<8; ++i) {
			byteVal[i] = (byte)((lng & ( 0xFF << i*8)) >> i*8);
		}
		return byteVal;
	}
	
	static public double EstimatedReach( String lastItem, int maxItems) {
		///long maxHash = HASH.hashUnencodedChars(lastItem).asLong();
		////long maxHash = HASH.hashString(lastItem).asLong();
		HashCode maxHash = HASH.hashString(lastItem);
		return EstimatedReach( maxHash, maxItems);
	}
	
	static public double EstimatedReach( long maxHash, int maxItems) {
       return EstimatedReach( BigInteger.valueOf(maxHash), maxItems, 8 );
	}

	static public double EstimatedReach( byte[] maxHash, int maxItems) {
       return EstimatedReach( new BigInteger(maxHash), maxItems, maxHash.length );
	}


	static public double EstimatedReach( HashCode maxHash, int maxItems) {
		return EstimatedReach( maxHash.asBytes(), maxItems);

	}

	/*    
	 *   |-----------------|------------------|
	 *          
	 * 
	 */
	static public double EstimatedReach( BigInteger maxHash, int maxItems, int byteLength) {
		BigInteger maxValue = MaxValueForByteLength( byteLength);
		
		BigDecimal maxHashShifted = new BigDecimal(maxHash.add( maxValue));
		///System.out.println(" MAX HASH  " + maxHash);
		///System.out.println(" MAX HASH SHIFTEd + " + maxHashShifted);
		BigInteger bigMaxItems = BigInteger.valueOf( maxItems*2).multiply( maxValue);
		BigDecimal ratio = new BigDecimal( bigMaxItems).divide(maxHashShifted,BigDecimal.ROUND_HALF_DOWN);
		///System.out.println(" RATIO = " + ratio);
		return ratio.doubleValue();
	}
	
	/**
	 *   Returns the maximum value for integers with a  given signed bytelength
	 *   
	 *   i.e For byteLength = 8, returns Long.MAX_VALUE
	 *   
	 * @param byteLength
	 * @return
	 */
	static public BigInteger MaxValueForByteLength( int byteLength ) {
		BigInteger allOnes = new BigInteger( new byte[] { Byte.MAX_VALUE } );
		BigInteger maxVal = allOnes;
		for(int i=0; i<byteLength -1; ++i ) {
			///System.out.println(" I = " + i + " maxVal = "+ maxVal);
			BigInteger shiftVal = maxVal.shiftLeft(8);
			maxVal = shiftVal.or( allOnes ).or( BigInteger.valueOf(128));
		}
	    ///System.out.println( "final maxVal = "+ maxVal);
		return maxVal;
	}
	
	static public BigInteger MinValueForByteLength( int byteLength ) {
		return MaxValueForByteLength(byteLength).negate();
	}
	
	/**
	 *  XXX SimHash probably broken with shift to 
	 *    bigger hashes 
	 *    TODO fixit
	 * @return
	 */
	public long calculateSimHash() {
		int[] sumTable = new int[ SIZEOF_LONG];
		
		Iterator<BigInteger> hashes = getHashItemMap().keySet().iterator();
		while( hashes.hasNext() ) {
			long hash =  hashes.next().longValue();

			byte mask = 1;
			for(int pos =0; pos < 8; ++pos ) {
				if( (hash & mask) != 0l) {
					sumTable[pos]++;
				} else {
					sumTable[pos]--;
				}
				mask <<=  1;
			}
		}
		long simHash = 0l;
		long mask = 1l;
		for(int pos=0; pos <SIZEOF_LONG; ++pos) {
			if( sumTable[pos] > 0) {
				simHash |= mask;
			}
			mask <<=1;
		}
		return simHash;
	}
	
	
	
	public void combine( SketchSet other) {
		for( Entry<BigInteger,String> entry: other.sortedMap.entrySet() ) {
			addHashItem( entry.getKey(), entry.getValue());
		}
	}
	
}