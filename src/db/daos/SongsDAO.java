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

import db.models.Songs;

public class SongsDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static SongsDAO instance = null;

	public static SongsDAO getInstance() {
		if (null == instance) {
			instance = new SongsDAO("demo");
		}
		return instance;
	}

	public static SongsDAO getNewInstance(String keyspaceName) {
		return new SongsDAO(keyspaceName);
	}

	private final String keyspaceName;

	private SongsDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void createTable() {
		String sql = String.format("CREATE TABLE %s.songs (id int,album text,artist text,data blob,tags set<text>,title text,PRIMARY KEY (id)) WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.1 AND gc_grace_seconds = 864000 AND bloom_filter_fp_chance = 0.01 AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' } AND comment = '' AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' } AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' } AND default_time_to_live = 0 AND speculative_retry = '99.0PERCENTILE' AND min_index_interval = 128 AND max_index_interval = 2048;", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void dropTable() {
		String sql = String.format("DROP TABLE %s.songs", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void insert(Songs obj) {
		String sql = String.format("INSERT INTO %s.songs (id, album, artist, data, tags, title) VALUES (?,?,?,?,?,?)", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(obj.id, obj.album, obj.artist, obj.data, obj.tags, obj.title);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	
	public <T> void setAddByID(long id, String fieldName, T content) {
		Set<T> target = new HashSet<T>();
		target.add(content);
		String sql = String.format("UPDATE %s.songs SET %s = %s + ? WHERE id = ?",
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
		String sql = String.format("UPDATE %s.songs SET %s = %s + ? WHERE id = ?",
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
		String sql = String.format("UPDATE %s.songs SET %s = %s - ? WHERE id = ?",
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
		String sql = String.format("UPDATE %s.songs SET %s = %s - ? WHERE id = ?",
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

	public List<Songs> selectAll() {
		String sql = String.format("SELECT * FROM %s.songs", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		List<Songs> result = new ArrayList<Songs>();
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

	public Songs selectById(long id) {
		String sql = String.format("SELECT * FROM %s.songs WHERE id = ?", keyspaceName);
		CassandraConnection conn = pool.getConnection();
		Songs result = null;
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

	private static Songs _constructResult(Row row) {
		Songs obj = new Songs();
		obj.id = row.getInt("id");
		obj.album = row.getString("album");
		obj.artist = row.getString("artist");
		obj.data = row.getBytes("data");
		obj.tags = row.getSet("tags", String.class);
		obj.title = row.getString("title");

		return obj;
	}
}
