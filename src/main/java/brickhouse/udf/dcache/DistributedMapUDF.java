package brickhouse.udf.dcache;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.log4j.Logger;

/**
 *   UDF to access a distributed map file 
 *   
 *   Assumes the file is a tab-separated file of name-value pairs,
 *   which has been placed in distributed cache using the "add file" command
 * 
 * Example 
 * 
 *  INSERT OVERWRITE LOCAL DIRECTORY mymap select key,value from my_map_table;
 *  ADD FILE mymap;
 *  
 *  select key, val* distributed_map( key, 'mymap') from the_table;
 *   
 *
 */
@UDFType(deterministic=false)
public class DistributedMapUDF extends UDF {
	private static final Logger LOG = Logger.getLogger(DistributedMapUDF.class);
	private static HashMap<String,HashMap<String,Double>> localMapMap = new HashMap<String,HashMap<String,Double>>();


	
	private void addValues(HashMap<String,Double> map, String mapFilename) throws IOException {
		if(!mapFilename.endsWith("crc")) {
			File mapFile  = new File( mapFilename);
			if( mapFile.isDirectory() ) {
				String[] subFiles = mapFile.list();
				for(String subFile : subFiles) {
					LOG.info( "Checking recursively " + subFile);
					addValues( map, mapFilename + "/" +subFile);
				}
			} else {
				BufferedReader reader = new BufferedReader(new InputStreamReader( new FileInputStream( mapFile)));

				String line;
				while( (line = reader.readLine() ) != null ) {
					LOG.info(" Line is "+ line);
					String[] fields = line.split("\001");
					if(fields.length >=2) {
						LOG.info(" ADDING " + fields[0] + " = " + fields[1]);
						Double dbl = Double.valueOf( fields[1]);
						map.put( fields[0], dbl);
					} else {
						LOG.warn(" Ignoring line " + line);
					}
				}
			}
		} else {
			LOG.info(" Ignoring CRC file " + mapFilename);
		}
	}
	
	
	public Double evaluate(String key, String mapFilename) {
	    HashMap<String,Double> map = localMapMap.get(mapFilename);
		if(map == null) {
			try {
				File localDir = new File(".");
				String[] files = localDir.list();
				for( String file : files) {
					LOG.info(" In current dir is " + file);
					File checkFile = new File(file);
					if(checkFile.isDirectory() ) {
						LOG.info(" FILE " + file + " is a directory");
					}
				}
				
				map = new HashMap<String,Double>();
				addValues( map,mapFilename);
				
				localMapMap.put( mapFilename, map);
			} catch(IOException ioExc) {
				ioExc.printStackTrace();
				throw new RuntimeException(ioExc);
			}
		}
		
		Double val =  map.get( key);
		return val;
	}
	
	/**
	 *  Same evaluate, but just return the entire map.
	 *   Assume that it is a Map<String,Double> that is returned,
	 *    since it is most likely some set of feature values ..
	 *    
	 *    TODO pass in type information on key values
	 * @param mapFileName
	 * @return
	 */
	public Map<String,Double> evaluate( String mapFilename ) {
	    HashMap<String, Double> map = localMapMap.get(mapFilename);
		if(map == null) {
			try {
				File localDir = new File(".");
				String[] files = localDir.list();
				for( String file : files) {
					LOG.info(" In current dir is " + file);
					File checkFile = new File(file);
					if(checkFile.isDirectory() ) {
						LOG.info(" FILE " + file + " is a directory");
					}
				}
				
				map = new HashMap<String,Double>();
				addValues( map,mapFilename);
				
				localMapMap.put( mapFilename, map);
			} catch(IOException ioExc) {
				ioExc.printStackTrace();
				throw new RuntimeException(ioExc);
			}
		}
		return map;
	}

}
