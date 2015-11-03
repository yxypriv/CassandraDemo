package db.daos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Lists;

import db.models.Teacher;

public class CassandraDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static CassandraDAO instance = null;

	public static CassandraDAO getInstance() {
		if (null == instance) {
			instance = new CassandraDAO("demo");
		}
		return instance;
	}

	public static CassandraDAO getNewInstance(String keyspaceName) {
		return new CassandraDAO(keyspaceName);
	}

	private final String keyspaceName;

	private CassandraDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public List<TableMetadata> showTable() {
		CassandraConnection conn = pool.getConnection();
		Collection<TableMetadata> tables = conn.getTables();
		List<TableMetadata> result = new ArrayList<TableMetadata>();
		result.addAll(tables);
		conn.close();
		return result;
	}

	public TableMetadata showTable(String tableName) {
		CassandraConnection conn = pool.getConnection();
		TableMetadata result = conn.getTable(tableName);
		conn.close();
		return result;
	}

	public static void main(String[] args) {
		CassandraDAO dao = CassandraDAO.getInstance();
		dao.showTable();
		ConnectionPool.getInstance().close();

	}
}
