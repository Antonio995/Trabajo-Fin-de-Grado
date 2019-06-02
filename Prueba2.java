package JDBC;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Function;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;

import java.io.File;
import java.util.HashMap;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;


public class Prueba2 {

	public static void main(String[] args) throws ClassNotFoundException {
		// TODO Auto-generated method stub
  	  long t = System.currentTimeMillis();


        Class.forName("com.leanxcale.jdbc.ElasticDriver");
        
        //realizar consulta
		SparkSession sparkSession = new SparkSession.Builder().appName("Spark Prueba2")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> jdbcDF = sparkSession.read()
				.format("jdbc")
				.option("url","jdbc:leanxcale:direct://34.244.196.192:1529/db")
				.option("driver","com.leanxcale.jdbc.ElasticDriver")
				.option("dbtable","(SELECT Activity_Period,Geo_Region, Passenger_Count FROM bbdd WHERE Activity_Period >= DATE('2005-12-02')) AS t")
				.load();
  	  long tsql = System.currentTimeMillis();

      System.out.println("Finaliza consulta, con un tiempo de: "+ (double)(tsql - t)/1000 + " segundos");


		//jdbcDF.show();
      
        //transformación 
		
		JavaRDD<Row> r = jdbcDF.toJavaRDD();
 /*
		 for(Row line:r.collect()) {
	        	System.out.println("Activity_Period:"+line.get(0) + ", Geo_Region:"+line.get(1) + ", Passenger_Count"+line.get(2));
	        }
*/	 
		 JavaPairRDD<String,Integer>  tra2 = r.mapToPair(x -> new Tuple2("Fecha:"+x.getDate(0)+" y Origen:"+x.getString(1),x.getInt(2)));
/*
	        tra2.foreach(data ->{
	        	System.out.println(data._1()+" Personas:"+data._2());
	        });
*/   
	        //reduceByKey
	        Function2<Integer,Integer,Integer> reduceSumFunc = (acum, n) -> (acum + n);
	        JavaPairRDD<String,Integer> traFin = tra2.reduceByKey(reduceSumFunc);
	        
	        //print tuples:

	        for(Tuple2<String,Integer> element:traFin.collect()) {
	        	System.out.println("("+element._1+", Personas: "+element._2+")");
	        }
	        
	    	  long tfin = System.currentTimeMillis();

	          System.out.println("Finaliza la prueba, con un tiempo de: "+ (double)(tfin - t)/1000 + " segundos");



	}

}
