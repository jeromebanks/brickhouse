package brickhouse.udf.hll;
/**
 * Copyright 2012,2013 Klout, Inc
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

import java.io.IOException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

class HLLBuffer implements AggregationBuffer {
	private ICardinality hll;
	private int precision;
	
	

	public void init(int precision) {
		this.precision = precision;
		hll = new HyperLogLogPlus(precision );
	}
	
	public void reset() {
		init( precision);
	}
    
    public void addItem( String str) {
    	hll.offer( str);
    }

    public void merge( byte[] buffer) throws IOException, CardinalityMergeException {
    	ICardinality other = HyperLogLogPlus.Builder.build(buffer);
    	hll.merge( other);
    }
    
    public byte[] getPartial() throws IOException {
    	return hll.getBytes();
    }


}