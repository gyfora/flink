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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * 
 * Adapter for bridging inconsistencies between the different SQL
 * implementations.
 *
 */
public class DbAdapter implements Serializable {

	private static final long serialVersionUID = 1L;

	// -----------------------------------------------------------------------------
	// Non-partitioned state checkpointing
	// -----------------------------------------------------------------------------

	public void createCheckpointsTable(String jobId, Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE IF NOT EXISTS checkpoints_" + jobId
							+ " ("
							+ "checkpointId bigint, "
							+ "timestamp bigint, "
							+ "handleId bigint,"
							+ "serializedData blob,"
							+ "PRIMARY KEY (handleId)"
							+ ")");
		}

	}

	public PreparedStatement prepareCheckpointInsert(String jobId, Connection con) throws SQLException {
		return con.prepareStatement(
				"INSERT INTO checkpoints_" + jobId
						+ " (checkpointId, timestamp, handleId, serializedData) VALUES (?,?,?,?)");
	}

	public void setCheckpointInsertParams(String jobId, PreparedStatement insertStatement, long checkpointId,
			long timestamp,
			long handleId, byte[] checkpoint) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setLong(2, timestamp);
		insertStatement.setLong(3, handleId);
		insertStatement.setBytes(4, checkpoint);
	}

	public byte[] getCheckpoint(String jobId, Connection con, long checkpointId, long handleId) throws SQLException {
		try (Statement smt = con.createStatement()) {
			ResultSet rs = smt.executeQuery(
					"SELECT serializedData FROM checkpoints_" + jobId
							+ " WHERE handleId = " + handleId);
			if (rs.next()) {
				return rs.getBytes(1);
			} else {
				throw new SQLException("Checkpoint cannot be found in the database.");
			}
		}
	}

	public void deleteCheckpoint(String jobId, Connection con, long checkpointId, long handleId) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DELETE FROM checkpoints_" + jobId
							+ " WHERE handleId = " + handleId);
		}
	}

	public void disposeAllStateForJob(String jobId, Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DROP TABLE checkpoints_" + jobId);
		}
	}

	// -----------------------------------------------------------------------------
	// Partitioned state checkpointing
	// -----------------------------------------------------------------------------

	public void createKVStateTable(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE IF NOT EXISTS kvstate_" + stateId
							+ " ("
							+ "checkpointId bigint, "
							+ "timestamp bigint, "
							+ "k varbinary(256), "
							+ "v blob, "
							+ "PRIMARY KEY (k, checkpointId, timestamp), "
							+ "KEY cleanup (checkpointId, timestamp)"
							+ ")");
		}
	}

	public PreparedStatement prepareKeyLookup(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		return con.prepareStatement("SELECT v"
				+ " FROM kvstate_" + stateId
				+ " WHERE k = ?"
				+ " AND checkpointId <= ?"
				+ " AND timestamp <= ?"
				+ " ORDER BY checkpointId DESC LIMIT 1");
	}

	public byte[] lookupKey(String stateId, PreparedStatement lookupStatement, byte[] key, long lookupId,
			long lookupTs) throws SQLException {
		lookupStatement.setBytes(1, key);
		lookupStatement.setLong(2, lookupId);
		lookupStatement.setLong(3, lookupTs);

		ResultSet res = lookupStatement.executeQuery();

		if (res.next()) {
			return res.getBytes(1);
		} else {
			return null;
		}
	}

	public PreparedStatement prepareKVCheckpointInsert(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		return con.prepareStatement(
				"INSERT INTO kvstate_" + stateId + " (checkpointId, timestamp, k, v) VALUES (?,?,?,?)");
	}

	public void setKVCheckpointInsertParams(String stateId, PreparedStatement insertStatement, long checkpointId,
			long timestamp,
			byte[] key, byte[] value) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setLong(2, timestamp);
		insertStatement.setBytes(3, key);
		if (value != null) {
			insertStatement.setBytes(4, value);
		} else {
			insertStatement.setNull(4, Types.BLOB);
		}
	}

	public void cleanupFailedCheckpoints(String stateId, Connection con, long lookupId, long lookupTs)
			throws SQLException {
		validateStateId(stateId);
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate("DELETE FROM kvstate_" + stateId
					+ " WHERE checkpointId > " + lookupId
					+ " AND timestamp < " + lookupTs);
		}
	}

	public static void validateStateId(String name) {
		if (!name.matches("[a-zA-Z0-9_]+")) {
			throw new RuntimeException("State name contains invalid characters.");
		}
	}

}
