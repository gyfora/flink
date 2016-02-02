/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

public class Interval implements Comparable<Interval>, Serializable {
	private static final long serialVersionUID = 1L;
	public final long from;
	public final long to;

	private Interval(long from, long to) {
		if (from <= to) {
			this.from = from;
			this.to = to;
		} else {
			throw new IllegalArgumentException("Interval startpoint cannot be greater than the endpoint.");
		}
	}

	public boolean overlapsWith(Interval other) {
		return !isBefore(other) && !isAfter(other);
	}

	public boolean isBefore(Interval other) {
		return to < other.from;
	}

	public boolean isAfter(Interval other) {
		return from > other.to;
	}

	@Override
	public int compareTo(Interval other) {
		return isBefore(other) ? 1 : isAfter(other) ? -1 : 0;
	}

	public static Interval closed(long from, long to) {
		return new Interval(from, to);
	}

	public static Interval point(long p) {
		return closed(p, p);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (from ^ (from >>> 32));
		result = prime * result + (int) (to ^ (to >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Interval)) {
			return false;
		}
		Interval other = (Interval) obj;
		return compareTo(other) == 0;
	}

	public static Interval pathToInterval(Path path) {
		String fname = path.getName();
		String[] split = fname.split("_");
		if (split[0].equals("merged")) {
			return Interval.closed(Long.parseLong(split[1]), Long.parseLong(split[2]));
		} else if (split[0].equals("cp")) {
			return Interval.point(Long.parseLong(split[1]));
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		return "[" + from + ", " + to + "]";
	}

}
