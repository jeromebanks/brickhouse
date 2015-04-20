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


import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class SketchSet implements ICountDistinct {
    static final int SIZEOF_LONG = 64;

    public static int DEFAULT_MAX_ITEMS = 5000;
    private int maxItems = DEFAULT_MAX_ITEMS;
    private TreeMap<Long, String> sortedMap;
    private static HashFunction HASH = Hashing.md5();


    public SketchSet() {
        sortedMap = new TreeMap<Long, String>();
    }


    public SketchSet(int max) {
        this.maxItems = max;
        sortedMap = new TreeMap<Long, String>();
    }

    public void addHashItem(long hash, String str) {
        if (sortedMap.size() < maxItems) {
            sortedMap.put(hash, str);
        } else {
            Long hashLong = hash;
            if (!sortedMap.containsKey(hashLong)) {
                long maxHash = sortedMap.lastKey();
                if (hash < maxHash) {
                    sortedMap.remove(maxHash);
                    sortedMap.put(hashLong, str);
                }
            }
        }
    }

    /**
     * for testing
     *
     * @param hash
     */
    public void addHash(long hash) {
        addHashItem(hash, Long.toString(hash));
    }

    public void addItem(String str) {
        HashCode hc = HASH.hashUnencodedChars(str);
        this.addHashItem(hc.asLong(), str);
    }

    public List<String> getMinHashItems() {
        return new ArrayList(this.sortedMap.values());
    }

    public SortedMap<Long, String> getHashItemMap() {
        return this.sortedMap;
    }

    public List<Long> getMinHashes() {
        return new ArrayList(this.sortedMap.keySet());
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

    public String lastItem() {
        return sortedMap.lastEntry().getValue();
    }

    public double estimateReach() {
        if (sortedMap.size() < maxItems) {
            return sortedMap.size();
        }
        long maxHash = sortedMap.lastKey();
        return EstimatedReach(maxHash, maxItems);
    }

    static public double EstimatedReach(String lastItem, int maxItems) {
        long maxHash = HASH.hashUnencodedChars(lastItem).asLong();
        return EstimatedReach(maxHash, maxItems);
    }

    static public double EstimatedReach(long maxHash, int maxItems) {
        BigDecimal maxHashShifted = new BigDecimal(BigInteger.valueOf(maxHash).add(BigInteger.valueOf(Long.MAX_VALUE)));

        BigDecimal bigMaxItems = new BigDecimal(maxItems * 2).multiply(BigDecimal.valueOf(Long.MAX_VALUE));
        BigDecimal ratio = bigMaxItems.divide(maxHashShifted, RoundingMode.HALF_EVEN);
        return ratio.doubleValue();
    }


    public long calculateSimHash() {
        int[] sumTable = new int[SIZEOF_LONG];

        Iterator<Long> hashes = getHashItemMap().keySet().iterator();
        while (hashes.hasNext()) {
            long hash = hashes.next();
            long mask = 1l;
            for (int pos = 0; pos < SIZEOF_LONG; ++pos) {
                if ((hash & mask) != 0l) {
                    sumTable[pos]++;
                } else {
                    sumTable[pos]--;
                }
                mask <<= 1;
            }
        }
        long simHash = 0l;
        long mask = 1l;
        for (int pos = 0; pos < SIZEOF_LONG; ++pos) {
            if (sumTable[pos] > 0) {
                simHash |= mask;
            }
            mask <<= 1;
        }
        return simHash;
    }


    public void combine(SketchSet other) {
        for (Entry<Long, String> entry : other.sortedMap.entrySet()) {
            addHashItem(entry.getKey(), entry.getValue());
        }
    }

}