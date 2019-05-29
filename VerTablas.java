package JDBC;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class VerTablas {
	
	private static String driver = "com.leanxcale.jdbc.ElasticDriver";
	private static String protocol = "jdbc:leanxcale:direct://";
	private static String cluster="34.244.196.192:1529";

	
	public static void main(String[] args) throws SQLException {
		
		
		System.out.println("SimpleApp starting in.");
		
		Connection conn = null;
		PreparedStatement psInsert = null;
		PreparedStatement psUpdate = null;
		Statement s = null;
		ResultSet rs = null;
		
		Properties props = new Properties(); // connection properties
		String dbName = "db"; // the name of the database
		
		conn = DriverManager.getConnection(protocol + cluster + "/"+ dbName );
		System.out.println("Connected to " + dbName);
		
		conn.setAutoCommit(false);

		/* Creating a statement object that we can use for running
		* various SQL statements commands against the database.*/
		s = conn.createStatement();
		
		
		// We create a table...

		//s.execute("create table prueba2 (numero int, calle varchar(40))");
		
		//System.out.println("tabla prueba creada");
		
		/*
		
		psInsert = conn.prepareStatement("insert into prueba2 values (?, ?)");
		psInsert.setInt(1, 1111);
		psInsert.setString(2, "Webmail");
		psInsert.executeUpdate();
		System.out.println("Meto 1111,Webmail");
		
		psInsert.setInt(1, 1212);
		psInsert.setString(2, "Hola");
		psInsert.executeUpdate();
		System.out.println("Meto 1212,Hola");
		
		psInsert.setInt(1, 1313);
		psInsert.setString(2, "que tal");
		psInsert.executeUpdate();
		System.out.println("Meto 1313,que tal");
		
		
		rs = s.executeQuery(
				"SELECT * from prueba2 ORDER BY numero");
		while (rs.next()) {
			System.out.println("Tiene como int: "+rs.getString("numero")+", String: "+rs.getString("calle"));
		}
		
		*/
		//see tables
		
		DatabaseMetaData md =conn.getMetaData();
		ResultSet rrr = md.getTables(null, null, "%", null);
		while(rrr.next()) {
			System.out.println(rrr.getString(3));
		}
		DatabaseMetaData md2 =conn.getMetaData();
		ResultSet rrr2 = md2.getColumns(null, null, "PRUEBA", null);
		System.out.println("imprime tabla ......");

		while(rrr2.next()) {
			System.out.println("Nombre: "+rrr2.getString("COLUMN_NAME")+", Tipo: "+rrr2.getString("DATA_TYPE")+", Capacidad: "+rrr2.getString("COLUMN_SIZE"));
		}
		
		
//		System.out.println("Vamos a eliminar la tabla PRUEBA2");
//		s.execute("drop table tfg");

		
		
		
				
		conn.commit();
		conn.close();

		
	}


}
