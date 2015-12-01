/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Adapter for bridging inconsistencies between the different SQL
 * implementations. The default implementation has been tested to work well with
 * MySQL
 *
 */
public class MySqlAdapter implements DbAdapter {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(MySqlAdapter.class);

	private static final byte[] MARKER = new byte[0];

	// -----------------------------------------------------------------------------
	// Non-partitioned state checkpointing
	// -----------------------------------------------------------------------------

	@Override
	public void createCheckpointsTable(String jobId, Connection con) throws SQLException {
		if (!isTableCreated(con, "checkpoints_" + jobId)) {
			try (Statement smt = con.createStatement()) {
				smt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS checkpoints_" + jobId
								+ " ("
								+ "checkpointId bigint, "
								+ "timestamp bigint, "
								+ "handleId bigint,"
								+ "checkpoint blob,"
								+ "PRIMARY KEY (handleId)"
								+ ")");
			}
			LOG.debug("Checkpoints table created for {}", jobId);
		} else {
			LOG.debug("Checkpoints table alredy created for {}", jobId);
		}
	}

	@Override
	public PreparedStatement prepareCheckpointInsert(String jobId, Connection con) throws SQLException {
		return con.prepareStatement(
				"INSERT INTO checkpoints_" + jobId
						+ " (checkpointId, timestamp, handleId, checkpoint) VALUES (?,?,?,?)");
	}

	@Override
	public void setCheckpointInsertParams(String jobId, PreparedStatement insertStatement, long checkpointId,
			long timestamp, long handleId, byte[] checkpoint) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setLong(2, timestamp);
		insertStatement.setLong(3, handleId);
		insertStatement.setBytes(4, checkpoint);
	}

	@Override
	public byte[] getCheckpoint(String jobId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException {
		try (Statement smt = con.createStatement()) {
			ResultSet rs = smt.executeQuery(
					"SELECT checkpoint FROM checkpoints_" + jobId
							+ " WHERE handleId = " + handleId);
			if (rs.next()) {
				return rs.getBytes(1);
			} else {
				throw new SQLException("Checkpoint cannot be found in the database.");
			}
		}
	}

	@Override
	public void deleteCheckpoint(String jobId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DELETE FROM checkpoints_" + jobId
							+ " WHERE handleId = " + handleId);
		}
	}

	@Override
	public void disposeAllStateForJob(String jobId, Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DROP TABLE checkpoints_" + jobId);
		}
	}

	// -----------------------------------------------------------------------------
	// Partitioned state checkpointing
	// -----------------------------------------------------------------------------

	@Override
	public void createKVStateTable(String stateId, Connection con) throws SQLException {
		con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		validateStateId(stateId);
		if (!isTableCreated(con, stateId)) {
			try (Statement smt = con.createStatement()) {
				smt.executeUpdate(
						"CREATE TABLE IF NOT EXISTS " + stateId
								+ " ("
								+ "timestamp bigint, "
								+ "k varbinary(256), "
								+ "v blob, "
								+ "PRIMARY KEY (k, timestamp) "
								+ ")");
			}
			LOG.debug("KV checkpoint table created for {}", stateId);
		} else {
			LOG.debug("KV checkpoint table already created for {}", stateId);
		}
	}

	@Override
	public String prepareKVCheckpointInsert(String stateId) throws SQLException {
		validateStateId(stateId);
		return "INSERT IGNORE INTO " + stateId + " (timestamp, k, v) VALUES (?,?,?)";
	}

	@Override
	public String prepareKeyLookup(String stateId) throws SQLException {
		validateStateId(stateId);
		return "SELECT v"
				+ " FROM " + stateId
				+ " WHERE k = ?"
				+ " AND timestamp <= ?"
				+ " ORDER BY timestamp DESC LIMIT 1";
	}

	@Override
	public byte[] lookupKey(String stateId, PreparedStatement lookupStatement, byte[] key, long lookupTs)
			throws SQLException {
		lookupStatement.setBytes(1, key);
		lookupStatement.setLong(2, lookupTs);

		ResultSet res = lookupStatement.executeQuery();

		if (res.next()) {
			return res.getBytes(1);
		} else {
			return null;
		}
	}

	@Override
	public void cleanupFailedCheckpoints(final DbBackendConfig conf, final String stateId, final Connection con,
			final long checkpointTs,
			final long recoveryTs) throws SQLException {

		validateStateId(stateId);
		try {
			if (startCleanup(conf, stateId, con, recoveryTs)) {
				LOG.debug("Task selected for cleanup, cleaning " + stateId);
				SQLRetrier.retry(new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						try (Statement smt = con.createStatement()) {
							smt.executeUpdate("DELETE FROM " + stateId
									+ " WHERE timestamp > " + checkpointTs
									+ " AND timestamp < " + recoveryTs);
						}
						finishCleanup(conf, stateId, con, recoveryTs);
						return null;
					}
				}, conf.getMaxNumberOfSqlRetries());

				LOG.debug("Cleanup performed successfully for " + stateId);
			} else {
				waitForCleanup(conf, stateId, con, recoveryTs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not clean up state", e);
		}

	}

	private void waitForCleanup(DbBackendConfig conf, final String stateId, final Connection con, final long recoveryTs)
			throws IOException {
		int c = 0;
		boolean wait = true;
		while (wait) {
			c++;
			try {
				if (isCleanupFinished(conf, stateId, con, recoveryTs)) {
					wait = false;
				} else if (c > 60) {
					throw new IOException("Could not clean up in 30 seconds.");
				} else {
					LOG.debug("Some other task is already cleaning, sleeping...");
					Thread.sleep(500);
				}
			} catch (InterruptedException e) {
				wait = false;
			}
		}
	}

	private boolean startCleanup(DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs)
					throws IOException {
		return SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con
						.prepareStatement("INSERT IGNORE INTO " + stateId + " (timestamp, k, v) VALUES (?,?,?)")) {
					setKvInsertParams(stateId, smt, recoveryTs, MARKER, MARKER);
					return smt.executeUpdate() == 1;
				}
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	private void finishCleanup(final DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs) throws IOException {

		SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con
						.prepareStatement("UPDATE " + stateId + " SET v=? WHERE k=? AND timestamp=" + recoveryTs)) {
					smt.setNull(1, Types.BLOB);
					smt.setBytes(2, MARKER);
					smt.executeUpdate();
				}
				return null;
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	private boolean isCleanupFinished(final DbBackendConfig conf, final String stateId, final Connection con,
			final long recoveryTs)
					throws IOException {

		return SQLRetrier.retry(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try (PreparedStatement smt = con.prepareStatement("SELECT v FROM " + stateId
						+ " WHERE k = ? AND timestamp = " + recoveryTs)) {

					smt.setBytes(1, MARKER);
					ResultSet res = smt.executeQuery();

					if (res.next()) {
						return res.getBytes(1) == null;
					} else {
						throw new RuntimeException("Error in cleanup logic");
					}
				}
			}
		}, conf.getMaxNumberOfSqlRetries());
	}

	@Override
	public void compactKvStates(String stateId, Connection con, long lowerId, long upperId)
			throws SQLException {
		validateStateId(stateId);

		try (Statement smt = con.createStatement()) {
			smt.executeUpdate("DELETE state.* FROM " + stateId + " AS state"
					+ " JOIN"
					+ " ("
					+ " 	SELECT MAX(timestamp) AS maxts, k FROM " + stateId
					+ " 	WHERE timestamp BETWEEN " + lowerId + " AND " + upperId
					+ " 	GROUP BY k"
					+ " ) m"
					+ " ON state.k = m.k"
					+ " AND state.timestamp >= " + lowerId);
		}
	}

	/**
	 * Tries to avoid SQL injection with weird state names.
	 * 
	 */
	protected static void validateStateId(String name) {
		if (!name.matches("[a-zA-Z0-9_]+")) {
			throw new RuntimeException("State name contains invalid characters.");
		}
	}

	@Override
	public void insertBatch(final String stateId, final DbBackendConfig conf,
			final Connection con, final PreparedStatement insertStatement, final long checkpointTs,
			final List<Tuple2<byte[], byte[]>> toInsert, int partition) throws IOException {

		Long start = null;
		Long keySize = null;
		Long valueSize = null;
		if (LOG.isDebugEnabled()) {
			start = System.nanoTime();
			keySize = 0L;
			valueSize = 0L;
			for (Tuple2<byte[], byte[]> t : toInsert) {
				keySize += t.f0.length;
				valueSize += t.f1.length;
			}
			keySize = keySize / 1024;
			valueSize = valueSize / 1024;
		}

		SQLRetrier.retry(new Callable<Void>() {
			public Void call() throws Exception {
				for (Tuple2<byte[], byte[]> kv : toInsert) {
					setKvInsertParams(stateId, insertStatement, checkpointTs, kv.f0, kv.f1);
					insertStatement.addBatch();
				}
				insertStatement.executeBatch();
				insertStatement.clearBatch();
				return null;
			}
		}, conf.getMaxNumberOfSqlRetries(), conf.getSleepBetweenSqlRetries());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Inserted {} in {} ms, Table: {}, TKS: {} KB TRS: {} KB", toInsert.size(),
					(System.nanoTime() - start) / 1000000, stateId + "@" + conf.getShardUrl(partition), keySize,
					valueSize);
		}

	}

	protected void setKvInsertParams(String stateId, PreparedStatement insertStatement, long checkpointTs,
			byte[] key, byte[] value) throws SQLException {
		insertStatement.setLong(1, checkpointTs);
		insertStatement.setBytes(2, key);
		if (value != null) {
			insertStatement.setBytes(3, value);
		} else {
			insertStatement.setNull(3, Types.BLOB);
		}
	}

	@Override
	public void keepAlive(Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeQuery("SELECT 1");
		}
	}

	private boolean isTableCreated(Connection con, String tableName) throws SQLException {
		return con.getMetaData().getTables(null, null, tableName, null).next();
	}

}
