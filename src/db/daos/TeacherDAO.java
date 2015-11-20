package db.daos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Teacher;

public class TeacherDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static TeacherDAO instance = null;

	public static TeacherDAO getInstance() {
		if (null == instance) {
			instance = new TeacherDAO("demo");
		}
		return instance;
	}

	public static TeacherDAO getNewInstance(String keyspaceName) {
		return new TeacherDAO(keyspaceName);
	}

	private final String keyspaceName;

	private TeacherDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void createTable() {
		String sql = String.format("CREATE TABLE %s.teacher (id bigint,courses list<bigint>,name text,title text,PRIMARY KEY (id)) WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.1 AND gc_grace_seconds = 864000 AND bloom_filter_fp_chance = 0.01 AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' } AND comment = '' AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' } AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' } AND default_time_to_live = 0 AND speculative_retry = '99.0PERCENTILE' AND min_index_interval = 128 AND max_index_interval = 2048;", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void dropTable() {
		String sql = String.format("DROP TABLE %s.teacher", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void insert(Teacher obj) {
		String sql = String.format("INSERT INTO %s.teacher (id, courses, name, title) VALUES (?,?,?,?)", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(obj.id, obj.courses, obj.name, obj.title);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	
	public <T> void setAddByID(long id, String fieldName, T content) {
		Set<T> target = new HashSet<T>();
		target.add(content);
		String sql = String.format("UPDATE %s.teacher SET %s = %s + ? WHERE id = ?",
				keyspaceName, fieldName, fieldName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(target, (int) id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}

	public <T> void setAddByID(long id, String fieldName, Collection<T> content) {
		Set<T> target = new HashSet<T>(content);
		String sql = String.format("UPDATE %s.teacher SET %s = %s + ? WHERE id = ?",
				keyspaceName, fieldName, fieldName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(target, (int) id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	
	public <T> void setRemoveByID(long id, String fieldName, T content) {
		Set<T> target = new HashSet<T>();
		target.add(content);
		String sql = String.format("UPDATE %s.teacher SET %s = %s - ? WHERE id = ?",
				keyspaceName, fieldName, fieldName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(target, (int) id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}

	public <T> void setRemoveByID(long id, String fieldName, Collection<T> content) {
		Set<T> target = new HashSet<T>(content);
		String sql = String.format("UPDATE %s.teacher SET %s = %s - ? WHERE id = ?",
				keyspaceName, fieldName, fieldName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(target, (int) id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}

	public List<Teacher> selectAll() {
		String sql = String.format("SELECT * FROM %s.teacher", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		try {
			ResultSet rs = conn.execute(sql);
			for (Row row : rs) {
				result.add(_constructResult(row));
			}
		} finally {
			conn.close();
		}
		return result;
	}

	public Teacher selectById(long id) {
		String sql = String.format("SELECT * FROM %s.teacher WHERE id = ?", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		Teacher result = null;
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			if (rs.iterator().hasNext())
				result = _constructResult(rs.one());
		} finally {
			conn.close();
		}
		return result;
	}

	private static Teacher _constructResult(Row row) {
		Teacher obj = new Teacher();
		obj.id = row.getLong("id");
		obj.courses = row.getList("courses", Long.class);
		obj.name = row.getString("name");
		obj.title = row.getString("title");

		return obj;
	}
}
