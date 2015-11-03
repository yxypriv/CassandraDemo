package utils.connectionPool;

import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;

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
	
	public Collection<TableMetadata> getTables() {
		return getTables(keySpace);
	}
	
	public Collection<TableMetadata> getTables(String keyspace) {
		Metadata metadata = cluster.getMetadata();
		KeyspaceMetadata keySpaceMeta = metadata.getKeyspace(keyspace);
		Collection<TableMetadata> tables = keySpaceMeta.getTables();
		return tables;
	}
	
	public TableMetadata getTable(String tableName) {
		return getTable(keySpace, tableName);
	}
	
	public TableMetadata getTable(String keyspace, String tableName) {
		Metadata metadata = cluster.getMetadata();
		KeyspaceMetadata keySpaceMeta = metadata.getKeyspace(keyspace);
		return keySpaceMeta.getTable(tableName);
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
