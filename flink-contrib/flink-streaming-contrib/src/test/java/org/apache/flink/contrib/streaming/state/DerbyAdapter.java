/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Adapter for the Derby JDBC driver which has slightly restricted CREATE TABLE
 * and SELECT semantics compared to the default assumptions.
 * 
 */
public class DerbyAdapter extends DbAdapter {

	private static final long serialVersionUID = 1L;

	/**
	 * We need to override this method as Derby does not support the
	 * "IF NOT EXISTS" clause at table creation
	 */
	@Override
	public void createCheckpointsTable(String jobId, Connection con) throws SQLException {

		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE checkpoints_" + jobId
							+ " ("
							+ "checkpointId bigint, "
							+ "timestamp bigint, "
							+ "handleId bigint,"
							+ "checkpoint blob,"
							+ "PRIMARY KEY (handleId)"
							+ ")");
		} catch (SQLException se) {
			if (se.getSQLState().equals("X0Y32")) {
				// table already created, ignore
			} else {
				throw se;
			}
		}
	}

	/**
	 * We need to override this method as Derby does not support the
	 * "IF NOT EXISTS" clause at table creation
	 */
	@Override
	public void createKVStateTable(String stateId, Connection con) throws SQLException {

		validateStateId(stateId);
		try (Statement smt = con.createStatement()) {
			smt.executeUpdate(
					"CREATE TABLE kvstate_" + stateId
							+ " ("
							+ "timestamp bigint, "
							+ "k varchar(256) for bit data, "
							+ "v blob, "
							+ "PRIMARY KEY (k, timestamp)"
							+ ")");
		} catch (SQLException se) {
			if (se.getSQLState().equals("X0Y32")) {
				// table already created, ignore
			} else {
				throw se;
			}
		}
	}

	/**
	 * We need to override this method as Derby does not support "LIMIT n" for
	 * select statements.
	 */
	@Override
	public PreparedStatement prepareKeyLookup(String stateId, Connection con) throws SQLException {
		validateStateId(stateId);
		PreparedStatement smt = con.prepareStatement("SELECT v " + "FROM kvstate_" + stateId
				+ " WHERE k = ? "
				+ " AND timestamp <= ? "
				+ "ORDER BY timestamp DESC");
		smt.setMaxRows(1);
		return smt;
	}

}
