package example.write;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraDemo {
	Cluster cluster;
	Session session;

	public CassandraDemo() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		System.out.println(cluster.toString());
		session = cluster.connect();
	}

	public void createKeyspace(String key) { // create database
		PreparedStatement ps = session
				.prepare("CREATE KEYSPACE IF NOT EXISTS ? WITH replication "
						+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		BoundStatement bs = new BoundStatement(ps);
		session.execute(bs.bind(key));
	}

	public void useKeySpace(String key) {
		session = cluster.connect(key);
	}

	public void createTableDemo() {
		System.out.println("---Creating table----");
		session.execute("CREATE TABLE demo.songs (id int PRIMARY KEY,title text, album text,artist text,tags set<text>,data blob);");
	}

	public void alterTableDemo() {
		System.out.println("---Altering table----");
		session.execute("ALTER TABLE demo.songs DROP data");
	}

	public void dropTableDemo() {
		System.out.println("---Droping table----");
		session.execute("DROP TABLE demo.songs");
	}

	public void insertDemo() {
		System.out.println("---Inserting data----");
		session.execute("INSERT INTO demo.songs (id, title, album, artist, tags)"
				+ " VALUES (1, 'T1', 't1', 'a1', {'tag1', 'tag2'})");
		session.execute("INSERT INTO demo.songs (id, title, album, artist, tags)"
				+ " VALUES (2, 'T2', 't2', 'a2', {'tag2', 'tag3'})");
		session.execute("INSERT INTO demo.songs (id, title, album, artist, tags)"
				+ " VALUES (3, 'T3', 't4', 'a3', {'tag1', 'tag3'})");
	}

	public void selectBySetConstrainDemo() {
		System.out.println("---Fetching data----");
		ResultSet rs = session.execute("SELECT * FROM demo.songs WHERE tags CONTAINS 'tag3'");
		for(Row row : rs) {
			System.out.println(row.getString("title") + "\t" + row.getString("artist"));
		}
	}
	
	public void selectDemo() {
		System.out.println("---Select data by tag----");
		ResultSet rs = session.execute("SELECT * FROM demo.songs");
		for(Row row : rs) {
			System.out.println(row.getString("title") + "\t" + row.getString("artist"));
		}
	}
	
	public void deleteDemo() {
		System.out.println("---Deleting data----");
		session.execute("DELETE FROM demo.songs WHERE id = 2");
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public static void main(String[] args) {
		String key = "demo";
		CassandraDemo demo = new CassandraDemo();
		// demo.createKeyspace(key);
		demo.useKeySpace(key);
		try {
//			demo.createTableDemo();
//			demo.alterTableDemo();
//			demo.insertDemo();
			demo.selectDemo();
//			demo.selectBySetConstrainDemo();
//			demo.deleteDemo();
//			demo.selectDemo();
		} catch (com.datastax.driver.core.exceptions.AlreadyExistsException e) {
			e.printStackTrace();
		} finally {
//			demo.dropTableDemo();
			demo.close();
		}
	}
}
