package KiVi;

import java.util.regex.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.leanxcale.kivi.database.Database;
import com.leanxcale.kivi.database.Index;
import com.leanxcale.kivi.database.Table;
import com.leanxcale.kivi.query.TupleIterable;
import com.leanxcale.kivi.session.Connection;
import com.leanxcale.kivi.session.ConnectionFactory;
import com.leanxcale.kivi.session.Credentials;
import com.leanxcale.kivi.session.Settings;
import com.leanxcale.kivi.tuple.Tuple;
import com.leanxcale.kivi.tuple.TupleKey;

import scala.Tuple2;

import javax.transaction.TransactionRequiredException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static com.leanxcale.kivi.query.aggregation.Aggregations.avg;
import static com.leanxcale.kivi.query.expression.Constants.integer;
import static com.leanxcale.kivi.query.expression.Constants.string;
import static com.leanxcale.kivi.query.expression.Constants.values;
import static com.leanxcale.kivi.query.expression.Expressions.field;
import static com.leanxcale.kivi.query.expression.Expressions.len;
import static com.leanxcale.kivi.query.filter.Filters.between;
import static com.leanxcale.kivi.query.filter.Filters.eq;
import static com.leanxcale.kivi.query.filter.Filters.in;
import static com.leanxcale.kivi.query.filter.Filters.ne;
import static com.leanxcale.kivi.query.filter.Filters.nullValue;
import static com.leanxcale.kivi.query.projection.Projections.exclude;
import static com.leanxcale.kivi.query.projection.Projections.include;
import static java.util.Collections.emptyList;

import java.io.IOException;
import java.sql.Date;
import java.util.regex.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.leanxcale.exception.LeanxcaleException;
import com.leanxcale.kivi.database.Database;
import com.leanxcale.kivi.database.Field;
import com.leanxcale.kivi.database.Index;
import com.leanxcale.kivi.database.Table;
import com.leanxcale.kivi.database.TableModel;
import com.leanxcale.kivi.database.Type;
import com.leanxcale.kivi.query.TupleIterable;
import com.leanxcale.kivi.session.Connection;
import com.leanxcale.kivi.session.ConnectionFactory;
import com.leanxcale.kivi.session.Credentials;
import com.leanxcale.kivi.session.Settings;
import com.leanxcale.kivi.tuple.Tuple;
import com.leanxcale.kivi.tuple.TupleKey;
import javax.transaction.TransactionRequiredException;


public class Prueba4 {
	

	    private static int indice = 0;

	  // TODO Auto-generated method stub
	  
		public static void main(String[] args) {
			
			try {
	        	  long tinic = System.currentTimeMillis();

				//-----CARGAR CSV CON SPARK----
	        	  
	            
	            SparkConf conf = new
	            		SparkConf().setAppName("fisrtSparkProyect")
	            		.setMaster("local[*]");

	           // @SuppressWarnings("resource")
	    		JavaSparkContext sc = new JavaSparkContext(conf);
	            //String path = "linescount.txt";

	            String path = "Air_Traffic_Passenger_Statistics.csv";
	            
	            System.out.println( "Procedemos a abrir el fichero: "+path );

	            JavaRDD<String> lines = sc.textFile(path.toString());

	            
	            JavaRDD<String> lines2 = lines.map(x ->añadeIndice(x));

	            long trdd = System.currentTimeMillis();

	            System.out.println("Ha tardado en cargar el CSV a RDD en "+ (double)(trdd-tinic)/1000 +" segundos.");

	            
				/* Step 1 - Create the connection with the direct connection with Lx-DB */
				Credentials credentials = new Credentials();
				credentials.setUser("APP");
				credentials.setDatabase("db");
				
				Settings settings = new Settings();
				settings.credentials(credentials);
				
				Connection connection;
				
					connection = ConnectionFactory.connect("kivi:zk://34.244.196.192:2181", settings);
				
				Database database = connection.database();
				
				System.out.println("conection.database");
			
				Table tableHost = database.getTable("TRAFFIC2");
				
				connection.beginTransaction();
			
				//meter datos en sql
			    int tcount = 1;
				  long t2 = System.currentTimeMillis();
				  
				    //for(String lin:lines2.collect()) {
				    	//System.out.println(lin);
			
				    //}
			
			
			    for(String lin:lines2.collect()) {
					String[]separado = lin.split(",");
				
				Tuple tuple = tableHost.createTuple();
				int numero = Integer.parseInt(separado[12]);

				int año=Integer.parseInt(separado[0].substring(0,4));
				int mes =Integer.parseInt(separado[0].substring(4));
				int fecha = año*65536+mes*256+1;
		      tuple.putInteger("ID", numero);
		      tuple.putInteger("ACTIVITY_PERIOD",fecha );
		      tuple.putString("OPERATING_AIRLINE", separado[1]);
		      tuple.putString("OPERATING_AIRLINE_IATA", separado[2]);
		      tuple.putString("PUBLISHED_AIRLINE", separado[3]);
		      tuple.putString("PUBLISHED_AIRLINE_IATA", separado[4]);
		      tuple.putString("GEO_SUMMARY", separado[5]);
		      tuple.putString("GEO_REGION", separado[6]);
		      tuple.putString("ACTIVITYTYPE_CODE", separado[7]);
		      tuple.putString("PRICE_CATEGORY_CODE", separado[8]);
		      tuple.putString("TERMINAL", separado[9]);
		      tuple.putString("BOARDING_AREA", separado[10]);
		      tuple.putInteger("PASSENGER_COUNT", Integer.parseInt(separado[11]));
		      tableHost.upsert(tuple);
		      tcount++;
     
		      
		      if (tcount > 100) {
		          connection.commit();

		          long tnow = System.currentTimeMillis();
		          System.err.println("Procesando carga ... - COMMIT: Throughput(Tuples written/second) " + (int)(tcount*1000L/(tnow - t2)));
		          t2 = tnow;
		          tcount = 0;
		          //Begin next transaction
		          connection.beginTransaction();

		      }

    }      
    if (tcount > 0) {
    connection.commit();
    }
    connection.close();
	  long t1 = System.currentTimeMillis();
      System.out.println("Ha tardado en cargar el CSV a RDD en "+ (double)(trdd-tinic)/1000 +" segundos.");
      System.out.println("Ha tardado en cargar los datos a la base de datos en "+ (double)(t1-trdd)/1000 +" segundos.");
      System.out.println("Ha tardado en realizar la prueba en "+ (double)(t1-tinic)/1000 +" segundos.");

		      
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LeanxcaleException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
		
}
		 public static String añadeIndice (String x){
				String res = x+","+Integer.toString(indice);
				indice++;
				return res;
			}
}
