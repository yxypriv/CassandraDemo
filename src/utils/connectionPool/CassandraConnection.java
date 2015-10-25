package utils.connectionPool;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class CassandraConnection {
	boolean occupied = false;
	boolean inPool = true;
	long lastUse = -1;

	String url;
	String keySpace;

	Cluster cluster = null;
	Session session = null;

	public CassandraConnection(String url, String keySpace) {
		super();
		this.url = url;
		this.keySpace = keySpace;
		initConnection();
	}

	public CassandraConnection(String url, String keySpace, boolean inPool) {
		super();
		this.url = url;
		this.keySpace = keySpace;
		this.inPool = inPool;
		initConnection();
	}

	private void initConnection() {
		cluster = Cluster.builder().addContactPoint(this.url).build();
		session = cluster.connect(this.keySpace);
		this.lastUse = System.currentTimeMillis();

	}

	public String getLoggedKeyspace() {
		return session.getLoggedKeyspace();
	}

	public PreparedStatement prepare(String query) {
		return session.prepare(query);
	}

	public PreparedStatement prepare(RegularStatement statement) {
		return session.prepare(statement);
	}

	public ResultSet execute(String query) {
		return execute(new SimpleStatement(query));
	}

    public ResultSet execute(Statement statement) {
        return session.execute(statement); 
	}
	
    public ResultSetFuture executeAsync(Statement statement) {
        return session.executeAsync(statement);
    }
    
	public void close() {
		if (!inPool) {
			session.close();
			cluster.close();
		} else {
			occupied = false;
		}
	}
	/**
	 * only used by connection pool to force close all connections in it;
	 */
	void forceClose() {
		session.close();
		cluster.close();
	}

	public boolean isClosed() {
		if (!inPool)
			return session.isClosed();
		else {
			return !occupied;
		}
	}
}
