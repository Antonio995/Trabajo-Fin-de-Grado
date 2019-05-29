package JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class Prueba22 {
	private static final ArrayList<String> data = new ArrayList<String>();
	private static String driver = "com.leanxcale.jdbc.ElasticDriver";
    private static String protocol = "jdbc:leanxcale:direct://";
    private static String cluster="34.244.196.192:1529";

    private static Connection conn = null;
    private static Statement s = null;
    private static PreparedStatement preparedStatement = null;
    private static ResultSet resultSet = null;
    private static int indice = 0;
    
public static Integer sacarnum(String x) {
		
		String[] separado = x.split(",");
		
		return Integer.valueOf(separado[2]);

		
	}
	
	public static String quitaArg(String x) {
		String[]separado = x.split(",");
		String res = "Fecha:"+separado[0]+", Origen:"+separado[1];
		return res;
	}
	
	private static void writeResultSet(ResultSet resultSet) throws SQLException, ParseException {
    	int n = 0;
        while (resultSet.next()) {
n++;
           String i1 = resultSet.getString("Activity_Period");
           String i2 = resultSet.getString("Geo_Region");
           String i3 = resultSet.getString("Passenger_Count");
           String f = i1+","+i2+","+i3;
           
           data.add(f);
           System.out.println(f);

        }
        System.out.println("NUmero de datos introducirdos: "+n);
    }



	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
        long t = System.currentTimeMillis();

        try {
        	//conexión a la db
    		String dbName = "db";
			conn = DriverManager.getConnection(protocol + cluster + "/"+ dbName );
		Properties props = new Properties();
		
		
		System.out.println("Connected to " + dbName);
		conn.setAutoCommit(false);
		s = conn.createStatement();
		
		//realizar consulta
		 preparedStatement = conn
                 .prepareStatement("SELECT Activity_Period,Geo_Region, Passenger_Count FROM bbdd WHERE Activity_Period >= DATE('2005-12-02')");
         resultSet = preparedStatement.executeQuery();
         
         //ver resultado

         writeResultSet(resultSet);
 		System.out.println("Connected to " + dbName);

 		} catch (SQLException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
        

        long tjdbc = System.currentTimeMillis();
        System.out.println("Finaliza consulta de datos con un talcon un tiempo de: "+ (double)(tjdbc - t)/1000);
  		
        //realizar algoritmo
  		SparkConf conf = new
          		SparkConf().setAppName("Spark jdbc 1")
          		.setMaster("local[*]");

         // @SuppressWarnings("resource")
  		JavaSparkContext sc = new JavaSparkContext(conf);
  		JavaRDD<String> items = sc.parallelize(data);
  		/*
  		for(String line:items.collect()) {
          	System.out.println(line);
          }
          */
  		
          JavaPairRDD<String,Integer>  tra2 = items.mapToPair(x -> new Tuple2(quitaArg(x),sacarnum(x)));
          
     
        //reduceByKey
          Function2<Integer,Integer,Integer> reduceSumFunc = (acum, p) -> (acum + p);
          JavaPairRDD<String,Integer> traFin = tra2.reduceByKey(reduceSumFunc);
          
          //print tuples:
          for(Tuple2<String,Integer> element:traFin.collect()) {
          	System.out.println("("+element._1+" , "+element._2+")");
          }
  	  	  long tspark = System.currentTimeMillis();
  	      System.out.println("Finaliza spark, con un tiempo de: "+ (double)(tspark - tjdbc)/1000 + " segundos");
  	      System.out.println("Finaliza prueba, con un tiempo de: "+ (double)(tspark - t)/1000 + " segundos");


	}
	
	
}
