//PRUEBA PARA INTRODUCIR TODOS LOS DATOS A LA TABLA TRAFFIC
package JDBC;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Prueba1 {
    private static String driver = "com.leanxcale.jdbc.ElasticDriver";
    private static String protocol = "jdbc:leanxcale:direct://";
    private static String cluster="34.244.196.192:1529";

    private static Connection conn = null;
    private static Statement statement = null;
    private static PreparedStatement preparedStatement = null;
    private static ResultSet resultSet = null;
    private static int indice = 0;


	public static void main(String[] args) throws Exception {
        try {
        	//tiempo desde el inicio
        	  long t = System.currentTimeMillis();

//-----CARGAR CSV CON SPARK----
        	  
            
            SparkConf conf = new
            		SparkConf().setAppName("fisrtSparkProyect")
            		.setMaster("local[*]");

           // @SuppressWarnings("resource")
    		JavaSparkContext sc = new JavaSparkContext(conf);

            String path = "Air_Traffic_Passenger_Statistics.csv";
            
            System.out.println( "Procedemos a abrir el fichero: "+path );

            JavaRDD<String> lines = sc.textFile(path.toString());

            
            JavaRDD<String> lines2 = lines.map(x ->añadeIndice(x));

            long trdd = System.currentTimeMillis();

            System.out.println("Ha tardado en cargar el CSV a RDD en "+ (double)(trdd-t)/1000 +" segundos.");
            
            for(String line:lines2.collect()) {
            	System.out.println(line);
            }
           
//CONECTARME A SQL AMAZON----- 


            PreparedStatement psInsert = null;
            PreparedStatement psUpdate = null;
            Statement s = null;
            ResultSet rs = null;
            
            Properties props = new Properties(); 
            String dbName = "db"; 
            
            conn = DriverManager.getConnection(protocol + cluster + "/"+ dbName );
            System.out.println("Connected to " + dbName);
            
            conn.setAutoCommit(false);

            statement = conn.createStatement();     
      

       //meter datos en sql
            int tcount = 1;
      	  long t2 = System.currentTimeMillis();

            for(String line:lines2.collect()) {
            	//System.out.println(line);
                tcount++;
            	
        		String[]separado = line.split(",");

            
            preparedStatement = conn.prepareStatement("insert into  tfg values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            int numero = Integer.parseInt(separado[12]);
            String inicio = separado[0];
            String da = separaFecha(inicio);
    		SimpleDateFormat df = new SimpleDateFormat("yyyy-M-dd : hh:mm:ss");
            Date d = df.parse(da);
            java.sql.Date x = new java.sql.Date(d.getTime());
            
            preparedStatement.setInt(1,numero);
            preparedStatement.setDate(2,x);
            preparedStatement.setString(3, separado[1]);
            preparedStatement.setString(4, separado[2]);
            preparedStatement.setString(5, separado[3]);
            preparedStatement.setString(6, separado[4]);
            preparedStatement.setString(7, separado[5]);
            preparedStatement.setString(8, separado[6]);
            preparedStatement.setString(9, separado[7]);
            preparedStatement.setString(10, separado[8]);
            preparedStatement.setString(11, separado[9]);
            preparedStatement.setString(12, separado[10]);
            preparedStatement.setInt(13, Integer.parseInt(separado[11]));
            preparedStatement.addBatch();
            //preparedStatement.executeUpdate();


            

            if(tcount >=100){
                long tnow = System.currentTimeMillis();
                preparedStatement.executeBatch();
            	conn.commit();
            System.out.println("Countinúa la carga de datos con un rendimiento de: "+ (int)(tcount*1000L/(tnow - t2))+" filas por segundo.");
            t2=tnow;
            tcount = 1;
            }
            
            }
            if (tcount > 0) {
            	conn.commit();
                preparedStatement.executeBatch();
            //preparedStatement.executeUpdate();
        }
    
            //ver resultados
           preparedStatement = conn
                    //.prepareStatement("SELECT * from bbdd WHERE PASSENGER_COUNT=357533");
                    // .prepareStatement("SELECT ID from traffic WHERE ID>=100000 GROUP BY ID ORDER BY ID");
                    .prepareStatement("SELECT * from traffic2 ");


            resultSet = preparedStatement.executeQuery();
            writeResultSet(resultSet);
            

            long tfin = System.currentTimeMillis();
            System.out.println("Finaliza la carga de datos con un tal de: "+ (double)(tfin - t)/1000);

 
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }

    public static String separaFecha (String s){
		return s.substring(0,4)+"-"+s.substring(4)+"-01 : 10:10:10";
	}
    
    public static String añadeIndice (String x){
		String res = x+","+Integer.toString(indice);
		indice++;
		return res;

	}
    
private static void writeResultSet(ResultSet resultSet) throws SQLException, ParseException {
    	int n = 0;
        while (resultSet.next()) {
            String i0 = resultSet.getString("ID");
n++;
           String i1 = resultSet.getString("Activity_Period");
           String i2 = resultSet.getString("Operating_Airline");
           String i3 = resultSet.getString("Operating_Airline_IATA");
           String i4 = resultSet.getString("Published_Airline");
           String i5 = resultSet.getString("Published_Airline_IATA");
           String i6 = resultSet.getString("Geo_Summary");
           String i7 = resultSet.getString("Geo_Region");
           String i8 = resultSet.getString("ActivityType_Code");
           String i9 = resultSet.getString("Price_Category_Code");
           String i10 = resultSet.getString("Terminal");
           String i11 = resultSet.getString("Boarding_Area");
           String i12 = resultSet.getString("Passenger_Count");
           
           SimpleDateFormat df = new SimpleDateFormat("yyyy-M-dd");
           Date d = df.parse(i1);
           System.out.println("que??? -> "+d.getTime());



            System.out.println("ID:" + i0 +""
            		+"Activity_Period: " + i1 + ""
            		+ "Operating_Airline: " + i2 + ""
            		+ "Operating_Airline_IATA: " + i3 + ""
            		+ "Published_Airline: " + i4 + ""
            		+ "Published_Airline_IATA: " + i5 + ""
            		+ "Geo_Summary: " + i6 + ""
            		+ "Geo_Region: " + i7 + ""
            		+ "ActivityType_Code: " + i8 + ""
            		+ "Price_Category_Code: " + i9 + ""
            		+ "Terminal: " + i10 + ""
            		+ "Boarding_Area: " + i11 + ""
            				+ "Passenger_Count: " + i12 + ""
            		
            		);
        }
        System.out.println("holaaaaa: "+n);
    }

    public static void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (conn != null) {
            	conn.close();
            }
        } catch (Exception e) {

        }
    }
    

}

