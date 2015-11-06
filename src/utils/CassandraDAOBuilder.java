package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

import db.daos.CassandraDAO;

public class CassandraDAOBuilder {
	String keyspace = "demo";
	String[] tableNames = { "songs" };
	String daoPathStr = "db/daos";
	String daoPackageStr = "db.daos";
	String modelPathStr = "db/models";
	String modelPackageStr = "db.models";
	String[] knownPaths = { "src/" };

	static Map<String, String> importImplementMapping = new HashMap<String, String>();
	static Set<String> daoExistingImportSet = new HashSet<String>();
	static final String ignoreImportPrefix = "java.lang";

	static {
		importImplementMapping.put("java.util.List", "java.util.ArrayList");
		importImplementMapping.put("java.util.Set", "java.util.HashSet");
		importImplementMapping.put("java.util.Map", "java.util.HashMap");

		daoExistingImportSet.add("java.util.List");
		daoExistingImportSet.add("java.util.ArrayList");
	}

	public static void main(String[] args) {
		CassandraDAOBuilder builder = new CassandraDAOBuilder();
		CassandraDAO dao = CassandraDAO.getInstance();
		// builder.buildModelFile(dao);
		builder.build(dao);
		ConnectionPool.getInstance().close();
	}

	public void build(CassandraDAO dao) {
		File projectRoot = new File("").getAbsoluteFile();
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

		if (null != tableNames) {
			for (String tableName : tableNames) {
				TableMetadata tableMeta = dao.showTable(tableName);

				Set<String> importSet = new HashSet<String>();
				for (ColumnMetadata column : tableMeta.getColumns()) {
					DataType type = column.getType();
					String className = StringsBuildUtil.getFullJavaName(type.asJavaClass());
					if (!className.startsWith(ignoreImportPrefix))
						importSet.add(className);
					if (type.isCollection()) {
						List<DataType> typeArguments = type.getTypeArguments();
						for (DataType dt : typeArguments) {
							String collectionClassName = StringsBuildUtil.getFullJavaName(dt
									.asJavaClass());
							if (!collectionClassName.startsWith(ignoreImportPrefix))
								importSet.add(collectionClassName);
						}
					}
				}

				String modelFileName = StringsBuildUtil.toCamelCase(tableName, true);
				buildModelFile(tableMeta, javaRoot, modelFileName, importSet);

				buildDAOFile(tableMeta, javaRoot, modelFileName, importSet);
			}
		}
	}

	private void buildDAOFile(TableMetadata tableMeta, File javaRoot, String modelFileName,
			Set<String> importSet) {
		File daosPath = new File(javaRoot, daoPathStr);
		File outputFile = new File(daosPath, modelFileName + "DAO.java");
		InputStream templateStream = CassandraDAOBuilder.class.getClassLoader()
				.getResourceAsStream("template");

		Map<String, String> replacementMap = new HashMap<String, String>();
		replacementMap.put("PACKAGE", daoPackageStr);
		replacementMap.put("MODEL_PACKAGE", modelPackageStr);
		replacementMap.put("MODEL_NAME", modelFileName);
		
		List<String> daoImports = new ArrayList<String>(daoExistingImportSet);
		for (String importStr : importSet) {
			if (!daoExistingImportSet.contains(importStr))
				daoImports.add(importStr);
		}
		Collections.sort(daoImports);
		StringBuilder importSB = new StringBuilder();
		for (String di : daoImports) {
			importSB.append(String.format("import %s;\r\n", di));
		}
		replacementMap.put("IMPORTS", importSB.toString());

		final StringBuilder sb = new StringBuilder();
		FileUtil.iterateStreamByLine(templateStream, new FileUtil.FileLineProcess() {
			@Override
			public boolean process(String line) {
				sb.append(line).append("\r\n");
				return true;
			}
		});
		String fileStr = sb.toString();
		for (String replaceKey : replacementMap.keySet()) {
			String replacement = replacementMap.get(replaceKey);
			fileStr = fileStr.replaceAll(String.format("%%\\{%s\\}%%", replaceKey),
					replacement);
		}
		FileUtil.writeToFile(outputFile, fileStr);
	}

	private void buildModelFile(TableMetadata tableMeta, File javaRoot, String fileName,
			Set<String> importSet) {
		File modelsPath = new File(javaRoot, modelPathStr);
		File outputFile = new File(modelsPath, fileName + ".java");

		PrintWriter writer = null;
		try {
			writer = new PrintWriter(outputFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		writer.println(String.format("package %s;", modelPackageStr));
		writer.println();

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
			if (name.equals("Set") || name.equals("List")) {
				List<DataType> typeArguments = type.getTypeArguments();
				name = String.format("%s<%s>", name,
						StringsBuildUtil.getShortJavaName(typeArguments.get(0).asJavaClass()));
			} else if (name.equals("Map")) {
				List<DataType> typeArguments = type.getTypeArguments();
				name = String.format("%s<%s, %s>", name,
						StringsBuildUtil.getShortJavaName(typeArguments.get(0).asJavaClass()),
						StringsBuildUtil.getShortJavaName(typeArguments.get(1).asJavaClass()));
			}
			writer.println(String.format("public %s %s;", name, column.getName()));
		}
		writer.println("}");
		writer.close();
	}
}
