package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

import db.daos.CassandraDAO;

public class CassandraDAOBuilder {
	String keyspace = "demo";
	String[] tableNames = { "songs" };
	String modelPathStr = "db/models";
	String packageDeclareStr = "db.models";

	static final String ignoreImportPrefix = "java.lang";
	
	public static void main(String[] args) {
		CassandraDAOBuilder builder = new CassandraDAOBuilder();
		builder.buildModelFile();
	}

	public void buildModelFile() {
		File projectRoot = new File("").getAbsoluteFile();
		String[] knownPaths = { "src/" };
		File javaRoot = null;
		for (String path : knownPaths) {
			javaRoot = new File(projectRoot, path);
			if (javaRoot.exists()) {
				break;
			}
		}
		if (null == javaRoot) {
			System.err.println("No Know Project Java Root Found");
			return;
		}

		File modelsPath = new File(javaRoot, modelPathStr);
		CassandraDAO dao = CassandraDAO.getInstance();
		if (null != tableNames) {
			for (String tableName : tableNames) {

				TableMetadata tableMeta = dao.showTable(tableName);
				String fileName = StringsBuildUtil.toCamelCase(tableName, true);
				File outputFile = new File(modelsPath, fileName + ".java");
				PrintWriter writer = null;
				try {
					writer = new PrintWriter(outputFile);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				writer.println(String.format("package %s;", packageDeclareStr));
				writer.println();

				Set<String> importSet = new HashSet<String>();
				for (ColumnMetadata column : tableMeta.getColumns()) {
					DataType type = column.getType();
					String className = StringsBuildUtil.getFullJavaName(type.asJavaClass());
					if (!className.startsWith(ignoreImportPrefix))
						importSet.add(className);
					if(type.isCollection()) {
						List<DataType> typeArguments = type.getTypeArguments();
						for(DataType dt : typeArguments) {
							String collectionClassName = StringsBuildUtil.getFullJavaName(dt.asJavaClass());
							if (!collectionClassName.startsWith(ignoreImportPrefix))
								importSet.add(collectionClassName);
						}
					}
				}
				List<String> imports = new ArrayList<String>(importSet);
				Collections.sort(imports);
				for (String is : imports) {
					writer.println(String.format("import %s;", is));
				}
				writer.println();

				int preTabs = 0;
				writer.println(String.format("public class %s {", fileName));
				preTabs++;
				for (ColumnMetadata column : tableMeta.getColumns()) {
					for (int i = 0; i < preTabs; i++)
						writer.print("\t");
					DataType type = column.getType();
					String name = StringsBuildUtil.getShortJavaName(type.asJavaClass());
					if(name.equals("Set")) {
						List<DataType> typeArguments = type.getTypeArguments();
						name = String.format("Set<%s>", StringsBuildUtil.getShortJavaName(typeArguments.get(0).asJavaClass()));
					}
					writer.println(String.format("public %s %s;", name, column.getName()));
				}
				writer.println("}");
				writer.close();
			}
		}
		ConnectionPool.getInstance().close();
	}
}
