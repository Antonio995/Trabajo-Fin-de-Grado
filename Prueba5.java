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
import com.leanxcale.kivi.database.Field;
import com.leanxcale.kivi.database.TableModel;
import com.leanxcale.kivi.database.Type;

public class Prueba5 {

	private static final Type String = null;
	
public static Integer sacarnum(String x) {
		
		String[] separado = x.split(",");
		
		return Integer.valueOf(separado[2]);

		
	}
	
	public static String quitaArg(String x) {
		String[]separado = x.split(",");
		String res = "Fecha:"+separado[0]+", Origen:"+separado[1];
		return res;
	}
	

	public static void main(String[] args) throws IOException, LeanxcaleException, ParseException {
	  	  long tini = System.currentTimeMillis();

		// TODO Auto-generated method stub
		
		/* Step 1 - Create the connection with the direct connection with Lx-DB */
		Credentials credentials = new Credentials();
		credentials.setUser("APP");
		credentials.setDatabase("db");
		
		Settings settings = new Settings();
		settings.credentials(credentials);
		
		Connection connection = ConnectionFactory.connect("kivi:zk://34.244.196.192:2181", settings);
		Database database = connection.database();
		
		System.out.println("conection.database");

		Table tableHost = database.getTable("BBDD");
		TupleIterable it = tableHost
				.find();
		
		//it.first(29);

		ArrayList<String>data = new ArrayList<String>();

		for(Tuple tpl:it) {
			//System.out.println("Un ID de:" +tpl.getInteger("ID"));
			//System.out.println(tpl.getFields());
			//System.out.println("Un activity_period dePasajerooos:" +tpl.getInteger("PASSENGER_COUNT"));
			//System.out.println("Una procedencia de:" +tpl.getString("OPERATING_AIRLINE"));
			//System.out.println("Un activity_period de:" +tpl.getDate("ACTIVITY_PERIOD"));

			int año = (tpl.getInteger("ACTIVITY_PERIOD") -1)/65536 ;
			int mes = (tpl.getInteger("ACTIVITY_PERIOD") -1-65536*año)/256 ;
			String fecha = Integer.toString(año)+"-"+Integer.toString(mes)+"-01";
			String dato = fecha+","+tpl.getString("GEO_REGION")+","+tpl.getInteger("PASSENGER_COUNT");
			if(año!=2005) {
				data.add(dato);
			}

		}
		
	it.close();
	  long tkivi = System.currentTimeMillis();
	/*
		Iterator ip = data.iterator();
		
		while(ip.hasNext()){
			System.out.println(ip.next());
		}
		*/
		
		SparkConf conf = new
        		SparkConf().setAppName("Spark KiVi 1")
        		.setMaster("local[*]");

       // @SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> items = sc.parallelize(data);
		
        JavaPairRDD<String,Integer>  tra2 = items.mapToPair(x -> new Tuple2(quitaArg(x),sacarnum(x)));
        
   
      //reduceByKey
        Function2<Integer,Integer,Integer> reduceSumFunc = (acum, p) -> (acum + p);
        JavaPairRDD<String,Integer> traFin = tra2.reduceByKey(reduceSumFunc);
        
        //print tuples:
        for(Tuple2<String,Integer> element:traFin.collect()) {
        	System.out.println("("+element._1+" , "+element._2+")");
        }
	  	  long tspark = System.currentTimeMillis();
	      System.out.println("Finaliza consulta, con un tiempo de: "+ (double)(tkivi - tini)/1000 + " segundos");
	      System.out.println("Finaliza spark, con un tiempo de: "+ (double)(tspark - tkivi)/1000 + " segundos");
	      System.out.println("Finaliza prueba, con un tiempo de: "+ (double)(tspark - tini)/1000 + " segundos");

	}
	
	

}
