package test;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class CassandraConnectionReuse {
	static int times = 100;
	static void accessTest1() {
		long t0 = System.currentTimeMillis();
		for(int i=0; i<times; i++) {
			Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
			Session conn = cluster.connect("demo");
			ResultSet rs = conn.execute("SELECT * FROM demo.songs");
			for(Row row : rs) {
//				System.out.println(row.getString("title") + "\t" + row.getString("artist"));
			}
			conn.close();
			cluster.close();
		}
		System.out.println(">>>>" + (System.currentTimeMillis() - t0) + "ms");
	}
	
	static void accessTest2() {
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		Session conn = cluster.connect("demo");
		long t0 = System.currentTimeMillis();
		for(int i=0; i<times; i++) {
			ResultSet rs = conn.execute("SELECT * FROM demo.songs");
			for(Row row : rs) {
//				System.out.println(row.getString("title") + "\t" + row.getString("artist"));
			}
		}
		conn.close();
		cluster.close();
		System.out.println(">>>>" + (System.currentTimeMillis() - t0) + "ms");
	}
	
	static void accessTest3() {
		ConnectionPool pool = ConnectionPool.getInstance();
		
		long t0 = System.currentTimeMillis();
		for(int i=0; i<times; i++) {
			CassandraConnection conn = pool.getConnection();
			ResultSet rs = conn.execute("SELECT * FROM demo.songs");
			for(Row row : rs) {
//				System.out.println(row.getString("title") + "\t" + row.getString("artist"));
			}
			conn.close();
		}
		pool.close();
		System.out.println(">>>>" + (System.currentTimeMillis() - t0) + "ms");
	}
	
	public static void main(String[] args) {
		accessTest1();
		accessTest2();
		accessTest3();
		
	}
}
