package utils.connectionPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConnectionPool {
	private static ConnectionPool instance = null;

	public static ConnectionPool getInstance() {
		ReadDBProperties propertiesReader = new ReadDBProperties();
		Properties properties = propertiesReader.getProperties();
		String url = properties.getProperty("db.url");
		String defaultUrlKeyspace = properties.getProperty("db.keyspace");
		if (null == instance)
			instance = new ConnectionPool(url, defaultUrlKeyspace);
		return instance;
	}

	private String url;
	private String keyspace;
	private List<CassandraConnection> pool = new ArrayList<CassandraConnection>();

	private int max = 30;

	public ConnectionPool(String url, String defaultKeySpace) {
		this.url = url;
		this.keyspace = defaultKeySpace;
	}

	public CassandraConnection getConnection() {
		CassandraConnection result = null;
		for (CassandraConnection conn : pool) {
			if (conn.occupied == false) {
				return conn;
			}
		}
		if (null == result) {
			if (pool.size() < max) {
				result = new CassandraConnection(url, keyspace);
				pool.add(result);
			} else {	// max out connections, create temp session.
				result = new CassandraConnection(url, keyspace, false);
			}
		}
		return result;
	}
	
	
	public void close() {
		for(CassandraConnection conn : pool) {
			conn.forceClose();
		}
		if(this == instance)
			instance = null;
	}
	/**
	 * TODO: add connection timeout check.
	 * TODO: add batch method.
	 */
}
