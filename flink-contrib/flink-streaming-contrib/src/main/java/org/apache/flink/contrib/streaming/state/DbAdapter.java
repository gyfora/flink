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
 * implementations. The default implementation has been tested to work well with
 * MySQL
 *
 */
public class DbAdapter implements Serializable {

	private static final long serialVersionUID = 1L;

	// -----------------------------------------------------------------------------
	// Non-partitioned state checkpointing
	// -----------------------------------------------------------------------------

	/**
	 * Initialize tables for storing non-partitioned checkpoints for the given
	 * job id and database connection.
	 * 
	 */
	public void createCheckpointsTable(String jobId, Connection con) throws SQLException {
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

	}

	/**
	 * Checkpoints will be inserted in the database using prepared statements.
	 * This methods should prepare and return the statement that will be used
	 * later to insert using the given connection.
	 * 
	 */
	public PreparedStatement prepareCheckpointInsert(String jobId, Connection con) throws SQLException {
		return con.prepareStatement(
				"INSERT INTO checkpoints_" + jobId
						+ " (checkpointId, timestamp, handleId, checkpoint) VALUES (?,?,?,?)");
	}

	/**
	 * Set the {@link PreparedStatement} parameters for the statement returned
	 * by {@link #prepareCheckpointInsert(String, Connection)}.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param insertStatement
	 *            Statement returned by
	 *            {@link #prepareCheckpointInsert(String, Connection)}.
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param timestamp
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @param checkpoint
	 *            The serialized checkpoint.
	 * @throws SQLException
	 */
	public void setCheckpointInsertParams(String jobId, PreparedStatement insertStatement, long checkpointId,
			long timestamp, long handleId, byte[] checkpoint) throws SQLException {
		insertStatement.setLong(1, checkpointId);
		insertStatement.setLong(2, timestamp);
		insertStatement.setLong(3, handleId);
		insertStatement.setBytes(4, checkpoint);
	}

	/**
	 * Retrieve the serialized checkpoint data from the database.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param con
	 *            Database connection
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param checkpointTs
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @return The byte[] corresponding to the checkpoint or null if missing.
	 * @throws SQLException
	 */
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

	/**
	 * Remove the given checkpoint from the database.
	 * 
	 * @param jobId
	 *            Id of the current job.
	 * @param con
	 *            Database connection
	 * @param checkpointId
	 *            Global checkpoint id.
	 * @param checkpointTs
	 *            Global checkpoint timestamp.
	 * @param handleId
	 *            Unique id assigned to this state checkpoint (should be primary
	 *            key).
	 * @return The byte[] corresponding to the checkpoint or null if missing.
	 * @throws SQLException
	 */
	public void deleteCheckpoint(String jobId, Connection con, long checkpointId, long checkpointTs, long handleId)
			throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DELETE FROM checkpoints_" + jobId
							+ " WHERE handleId = " + handleId);
		}
	}

	/**
	 * Remove all states for the given JobId, by for instance dropping the
	 * entire table.
	 * 
	 * @throws SQLException
	 */
	public void disposeAllStateForJob(String jobId, Connection con) throws SQLException {
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"DROP TABLE checkpoints_" + jobId);
		}
	}

	// -----------------------------------------------------------------------------
	// Partitioned state checkpointing
	// -----------------------------------------------------------------------------

	/**
	 * Initialize the necessary tables for the given stateId. The state id
	 * consist of the JobId+OperatorId+StateName.
	 * 
	 */
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

	/**
	 * Prepare the the statement that will be used to insert key-value pairs in
	 * the database.
	 * 
	 */
	public PreparedStatement prepareKVCheckpointInsert(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		return con.prepareStatement(
				"INSERT INTO kvstate_" + stateId + " (checkpointId, timestamp, k, v) VALUES (?,?,?,?)");
	}

	/**
	 * Insert new key-value pair in the database. The method should be able to
	 * receive null values which mark the removal of a key (tombstone).
	 * 
	 * @param stateId
	 *            Unique identifier of the kvstate (usually the table name).
	 * @param insertStatement
	 *            Statement prepared in
	 *            {@link #prepareKVCheckpointInsert(String, Connection)}
	 * @param checkpointId
	 * @param timestamp
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
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

	/**
	 * Prepare the statement that will be used to lookup keys from the database.
	 * Keys and values are assumed to be byte arrays.
	 * 
	 */
	public PreparedStatement prepareKeyLookup(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		return con.prepareStatement("SELECT v"
				+ " FROM kvstate_" + stateId
				+ " WHERE k = ?"
				+ " AND checkpointId <= ?"
				+ " AND timestamp <= ?"
				+ " ORDER BY checkpointId DESC LIMIT 1");
	}

	/**
	 * Retrieve the latest value from the database for a given key that has
	 * checkpointId <= lookupId and checkpointTs <= lookupTs.
	 * 
	 * @param stateId
	 *            Unique identifier of the kvstate (usually the table name).
	 * @param lookupStatement
	 *            The statement returned by
	 *            {@link #prepareKeyLookup(String, Connection)}.
	 * @param key
	 *            The key to lookup.
	 * @param lookupId
	 *            Latest checkpoint id to select.
	 * @param lookupTs
	 *            Latest checkpoint ts to select.
	 * @return The latest valid value for the key.
	 * @throws SQLException
	 */
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

	/**
	 * Remove partially failed snapshots using the latest id and a global
	 * recovery timestamp. All records with a higher id but lower timestamp
	 * should be deleted from the database.
	 * 
	 */
	public void cleanupFailedCheckpoints(String stateId, Connection con, long checkpointId, long reoveryTs)
			throws SQLException {
		validateStateId(stateId);
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate("DELETE FROM kvstate_" + stateId
					+ " WHERE checkpointId > " + checkpointId
					+ " AND timestamp < " + reoveryTs);
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

}
