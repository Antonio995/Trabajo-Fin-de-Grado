package JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

public class CreateTable {
	
	private static String driver = "com.leanxcale.jdbc.ElasticDriver";
	private static String protocol = "jdbc:leanxcale:direct://";
	private static String cluster="34.244.196.192:1529";


    private static Statement statement = null;
    private static PreparedStatement preparedStatement = null;
    private static ResultSet resultSet = null;
	private static Connection conn = null;





	public static void main(String[] args) throws Exception {
        try {
        	System.out.println("SimpleApp starting in.");
    		
    		Statement s = null;
    		
    		Properties props = new Properties(); // connection properties
    		String dbName = "db"; // the name of the database
    		
    		conn = DriverManager.getConnection(protocol + cluster + "/"+ dbName );
    		System.out.println("Connected to " + dbName);
    		
    		conn.setAutoCommit(false);

    		//Crear tabla
    		s = conn.createStatement();


            s.execute("CREATE TABLE prueba ("
            		+ "ID INT,"
            		+"Activity_Period DATE,"
            		+"Operating_Airline VARCHAR(50),"
            		+"Operating_Airline_IATA VARCHAR(5),"
            		+"Published_Airline VARCHAR(50),"
            		+"Published_Airline_IATA VARCHAR(5),"
            		+"Geo_Summary VARCHAR(20),"
            		+"Geo_Region VARCHAR(25),"
            		+"ActivityType_Code VARCHAR(25),"
            		+"Price_Category_Code VARCHAR(25),"
            		+"Terminal VARCHAR(20),"
            		+"Boarding_Area VARCHAR(5),"
            		+"Passenger_Count INT)"

            		);
            		
             System.out.println("Table created");
             conn.commit();
 			System.out.println("Committed the transaction");
     

         // Insertar instancia de mayor capacidad
            preparedStatement = conn
                    .prepareStatement("insert into  traffic values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            
            
			
            
            String inicio = "201809";
            //String da = "2018-09-01 : 10:00:00";
            String da = separaFecha(inicio);
            System.out.println("La fecha a insertar es: "+da);

    		SimpleDateFormat df = new SimpleDateFormat("yyyy-M-dd : hh:mm:ss");
            Date d = df.parse(da);
            
            java.sql.Date x = new java.sql.Date(d.getTime());
            
            preparedStatement.setInt(1,0);
            preparedStatement.setDate(2,x);
            preparedStatement.setString(3, "ABC Aerolineas S.A. de C.V. dba Interjet");
            preparedStatement.setString(4, "WW");
            preparedStatement.setString(5, "ABC Aerolineas S.A. de C.V. dba Interjet");
            preparedStatement.setString(6, "WW");
            preparedStatement.setString(7, "International");
            preparedStatement.setString(8, "Australia / Oceania");
            preparedStatement.setString(9, "Thru / Transit");
            preparedStatement.setString(10, "Low Fare");
            preparedStatement.setString(11, "International");
            preparedStatement.setString(12, "Other");
            preparedStatement.setInt(13, 659837);

            preparedStatement.executeUpdate();
            
            conn.commit();
			System.out.println("Committed2");
			

            //ver resultado
            preparedStatement = conn
                    .prepareStatement("SELECT * from traffic");
            resultSet = preparedStatement.executeQuery();
            writeResultSet(resultSet);


        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }
    public static String separaFecha (String s){
		return s.substring(0,4)+"-"+s.substring(4)+"-01 : 10:10:10";
	}

    private static void writeResultSet(ResultSet resultSet) throws SQLException {
    	
        while (resultSet.next()) {
            String i0 = resultSet.getString("ID");
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


            System.out.println("ID: " + i0 + "\n"
            		+ "Activity_Period: " + i1 + "\n"
            		+ "Operating_Airline: " + i2 + "\n"
            		+ "Operating_Airline_IATA: " + i3 + "\n"
            		+ "Published_Airline: " + i4 + "\n"
            		+ "Published_Airline_IATA: " + i5 + "\n"
            		+ "Geo_Summary: " + i6 + "\n"
            		+ "Geo_Region: " + i7 + "\n"
            		+ "ActivityType_Code: " + i8 + "\n"
            		+ "Price_Category_Code: " + i9 + "\n"
            		+ "Terminal: " + i10 + "\n"
            		+ "Boarding_Area: " + i11 + "\n"
            		+ "Passenger_Count: " + i12 + "\n"
            		
            		);
        }
    }

    private static void close() {
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
