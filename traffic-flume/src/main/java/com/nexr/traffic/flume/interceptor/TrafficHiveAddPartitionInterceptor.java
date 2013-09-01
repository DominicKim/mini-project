package com.nexr.traffic.flume.interceptor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.nexr.traffic.flume.common.FixedArrayList;
import com.nexr.traffic.flume.common.TrafficConstants;

public class TrafficHiveAddPartitionInterceptor implements Interceptor {

	private static final Logger logger = LoggerFactory.getLogger(TrafficHiveAddPartitionInterceptor.class);

	private final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	private final String PARTITION_DELIMITER = ",";
	private final String PATH_DELIMITER = "/";

	private final String hdfsPath;
	private final String hiveJdbcUrl;
	private final String hiveDatabase;
	private final String hiveTableName;
	private FixedArrayList<String> partitionCache;

	public TrafficHiveAddPartitionInterceptor(String hdfsPath, String hiveJdbcUrl, String hiveDatabase, String hiveTableName, Integer cacheSize) {
		this.hdfsPath = hdfsPath.endsWith(PATH_DELIMITER) ? hdfsPath : hdfsPath + PATH_DELIMITER;
		this.hiveJdbcUrl = hiveJdbcUrl;
		this.hiveDatabase = hiveDatabase;
		this.hiveTableName = hiveTableName;
		this.partitionCache = new FixedArrayList<String>(cacheSize.intValue());
	}

	@Override
	public void initialize() {
		try {
			Class.forName(HIVE_JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to load the hive jdbc driver");
		}
	}

	@Override
	public Event intercept(Event event) {
		if (event != null) {
			String date = event.getHeaders().get(TrafficConstants.HEADER_DATE);
			String hour = event.getHeaders().get(TrafficConstants.HEADER_HOUR);
			String partitionKey = date + PARTITION_DELIMITER + hour;

			if (!(this.partitionCache.contains(partitionKey))) {
				logger.debug("add new hive partitionKey : " + partitionKey);
				addPartition(partitionKey);
				this.partitionCache.add(partitionKey);
			}
		}
		return event;
	}

	private void addPartition(String partitionKey) {
	    String changeDbQuery = String.format("USE %s", this.hiveDatabase);

	    String[] partition = partitionKey.split(",");
	    String query = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (date='%s', hour='%s') LOCATION '%s%s/%s'",
					    			this.hiveTableName,
					    			partition[0], partition[1],
					    			this.hdfsPath, partition[0], partition[1]);

		Connection conn = null;
		try {
			conn = DriverManager.getConnection(this.hiveJdbcUrl);
		} catch (SQLException e) {
			new RuntimeException("Failed to connect Hive Server. hiveJdbcUrl : " + this.hiveJdbcUrl, e);
		}

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(changeDbQuery);
			logger.debug("HIVE execute query >>> " + changeDbQuery);
			stmt.execute(query);
			logger.debug("HIVE execute query >>> " + query);
		} catch (SQLException e) {
			new RuntimeException("Failed to create JDBC Statement", e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				new RuntimeException("Failed to close Hive Server", e);
			}
		}
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> out = Lists.newArrayList();
		for (Event event : events) {
			Event outEvent = intercept(event);
			if (outEvent != null) {
				out.add(outEvent);
			}
		}
		return out;
	}

	@Override
	public void close() {
		// no-op
	}

	public static class Builder implements Interceptor.Builder {
		private String hdfsPath;
		private String hiveJdbcUrl;
		private String hiveDatabase;
		private String hiveTableName;
		private Integer cacheSize;

		@Override
		public void configure(Context context) {
			hdfsPath = ((String) Preconditions.checkNotNull(context.getString("hdfs.path"), "hdfs.path is required"));
			hiveJdbcUrl = ((String) Preconditions.checkNotNull(context.getString("hive.jdbcUrl"),"Hive JDBC URL is required"));
			hiveDatabase = ((String) Preconditions.checkNotNull(context.getString("hive.database"),"Hive Database is required"));
			hiveTableName = ((String) Preconditions.checkNotNull(context.getString("hive.table"),"Hive Table Name is required"));
			cacheSize = context.getInteger("cache.size",Integer.valueOf(1000));			
		}

		@Override
		public Interceptor build() {
			logger.info("Creating TrafficHiveAddPartitionInterceptor");
		      return new TrafficHiveAddPartitionInterceptor(this.hdfsPath, this.hiveJdbcUrl, this.hiveDatabase, this.hiveTableName, this.cacheSize);
		}
	}

}
