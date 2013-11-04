package com.gopivotal.mapreduce.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class HawqUtil {

	private static final Logger LOG = Logger.getLogger(HawqUtil.class);

	public static Integer[] getColumnDefs(String url, String table, String user,
			String password) throws SQLException {

		try {
			// Load JDBC driver for Postgres
			Class.forName("org.postgresql.Driver");
			LOG.info("Loaded postgres JDBC driver");
		} catch (ClassNotFoundException e) {
			throw new SQLException("Postgres JDBC driver not on classpath");
		}

		Properties props = new Properties();

		if (user != null && password != null) {
			props.setProperty("user", user);
			props.setProperty("password", password);
		} else if (user != null ^ password != null) {
			LOG.warn("User or password is set without the other. Continuing with no login auth");
		}

		Connection client = DriverManager.getConnection(url, props);

		DatabaseMetaData data = client.getMetaData();
		ResultSet set = data.getColumns(null, null, table, null);

		List<Integer> types = new ArrayList<Integer>();
		while (set.next()) {
			types.add(set.getInt(5));
		}

		return types.size() > 0 ? types.toArray(new Integer[0]) : null;
	}
}
