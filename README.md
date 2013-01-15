Welcome to the Brickhouse
=========================

   Brickhouse is a collection of UDF's for Hive to improve developer 
   productivity, and the scalability and robustness of Hive queries.
   

  Brickhouse covers a wide range of functionality, grouped in the 
     following packages.

    "collect" - An implementaion of "collect"  and various utilities
      for dealing with maps and arrays.
   
    "json" - Translate between Hive structures and JSON strings

    "sketch" - An implementation of KMV sketch sets, for reach 
     estimation of large datasets.

    "bloom" - UDF wrappers around the Hadoop BloomFilter implementation.

    "sanity" - Tools for implementing sanity checks and managing Hive
	  in a production environment.
   
    "hbase" - Experimental UDFs for an alternative way to integrate
	  Hive with HBase.
     
Requirements:
  Brickhouse require Hive 0.9.0 or later
  Maven 2.0 and a Java JDK 6.0  is required to build.

Getting Started
---------------
 1. Clone ( or fork ) the repo from  https://github.com/klout/brickhouse 
 2. Run "mvn package" from the command line.
 3. Add the jar "target/brickhouse-<version>.jar" to your HIVE_AUX_JARS_FILE_PATH,
    or add it to the distributed cache from the Hive CLI 
    with the "add jar" command
 4. Source the UDF declarations defined in src/main/resource/brickhouse.hql

See the wiki on Github at https://github.com/klout/brickhouse/wiki for more 
  information.



