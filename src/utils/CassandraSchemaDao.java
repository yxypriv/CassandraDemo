package utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class CassandraSchemaDao {
	Cluster cluster;
	Session session;

	public CassandraSchemaDao() {
		cluster = Cluster.builder().addContactPoint("localhost")//
				.withMaxSchemaAgreementWaitSeconds(30)//
				.build();
		System.out.println(cluster.toString());
		session = cluster.connect();
	}

	public void showTableInfo(String keyspace, String table) {
		ResultSet rs = session.execute(String.format("DESCRIBE TABLE %s.%s", keyspace, table));
		for (Row row : rs) {
			System.out.println(row.toString());
		}
	}

	public TableMetadata getTableSchema(String keyspace, String table) {
		KeyspaceMetadata schema = cluster.getMetadata().getKeyspace(keyspace);
		return schema.getTable(table);
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public static void main(String[] args) {
		CassandraSchemaDao reader = new CassandraSchemaDao();
		System.out.println("Initializing finished~");
		try {
			TableMetadata tableSchema = reader.getTableSchema("demo", "songs");
			System.out.println(tableSchema.getName());
			for (ColumnMetadata column : tableSchema.getColumns()) {
				System.out.println("\t" + column.getName() + "\t" + column.getType());
			}
			System.out.println("show table finished");
		} finally {
			reader.close();
		}
	}
}
