package brickhouse.hbase;

import java.text.DecimalFormat;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *   Create a salted key from a BigInt, which 
 *    may not be distributed evenly across the 
 *     most significant bits ( ie. some large values, some low values)
 *     but are distributed evenly across the low bits
 *      ( ie. modulo 1000 )
 * @author jeromebanks
 *
 */
public class SaltedBigIntUDF extends UDF {
    private DecimalFormat saltFormat = new DecimalFormat("0000");

	public String evaluate( Long id ) {
        StringBuilder sb = new StringBuilder();	
        sb.append( saltFormat.format( id % 10000 ));
        sb.append(":");
        sb.append( id.toString());
		
        return sb.toString();
	}
}
