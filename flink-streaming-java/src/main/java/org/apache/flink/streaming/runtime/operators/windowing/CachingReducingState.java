/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache.
 */
public class CachingReducingState<V> implements ReducingState<V>, InternalAppendingState<Object, V, V> {

	static final Logger LOG = LoggerFactory.getLogger(CachingReducingState.class);

	private transient Map<Tuple2<Object, Object>, V> values;
	public final long maxSize;
	public final double flushPercentage;

	private final InternalAppendingState<Object, V, V> state;
	private final long numToFlush;

	private final AbstractStreamOperator<?> op;

	private final ReduceFunction<V> reducer;

	private Object currentNamespace;

	public CachingReducingState(InternalAppendingState<Object, V, V> state, AbstractStreamOperator<?> op, long maxSize,
			double flushPercentage,
			ReduceFunction<V> reducer) {
		this.state = state;
		this.op = op;
		this.maxSize = maxSize;
		this.flushPercentage = flushPercentage;
		this.reducer = reducer;
		this.values = flushPercentage == 1 ? new HashMap<Tuple2<Object, Object>, V>() : new LRUMap((int) maxSize);
		this.numToFlush = Math.round(flushPercentage * maxSize);
	}

	public void flush(double percentToFlush) throws Exception {
		Object prevKey = op.getCurrentKey();
		Object prevNameSpace = currentNamespace;

		if (percentToFlush == 1) {
			flushAll();
		} else {
			Iterator<Entry<Tuple2<Object, Object>, V>> entryIt = values.entrySet().iterator();
			int c = 0;
			while (entryIt.hasNext() && c++ < numToFlush) {
				writeToDb(entryIt.next());
				entryIt.remove();
			}

		}

		if (prevKey != null) {
			op.setCurrentKey(prevKey);
			//Narain: gettng null pointer when namespace is null
			if(prevNameSpace !=null)
			setCurrentNamespace(prevNameSpace);
		}

	}

	private void flushAll() throws Exception {
		for (Entry<Tuple2<Object, Object>, V> entry : values.entrySet()) {
			writeToDb(entry);
		}
		values.clear();
	}

	private void writeToDb(Entry<Tuple2<Object, Object>, V> entry) throws Exception {
		op.setCurrentKey(entry.getKey().f0);
		setCurrentNamespace(entry.getKey().f1);
		state.add(entry.getValue());
	}

	private void put(Object key, V value) throws Exception {
		values.put(Tuple2.of(key, currentNamespace), value);
		if (values.size() >= maxSize) {
			flush(flushPercentage);
		}
	}

	private class LRUMap extends LinkedHashMap<Tuple2<Object, Object>, V> {
		private static final long serialVersionUID = 1L;

		public LRUMap(int cacheSize) {
			super(cacheSize, 0.75f, true);
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<Tuple2<Object, Object>, V> eldest) {
			return false;
		}
	}

	@Override
	public void clear() {
		values.remove(getCacheKey());
		state.clear();
	}

	private Tuple2<Object, Object> getCacheKey() {
		return Tuple2.of(op.getCurrentKey(), currentNamespace);
	}

	@Override
	public void add(V next) throws Exception {
		V prev = values.get(getCacheKey());
		V reduced = prev == null ? next : reducer.reduce(prev, next);
		put(op.getCurrentKey(), reduced);
	}

	@Override
	public V get() throws Exception {
		V cached = values.get(getCacheKey());
		V inDb = state.get();
		return cached == null ? inDb : inDb == null ? cached : reducer.reduce(inDb, cached);
	}

	@Override
	public byte[] getSerializedValue(byte[] key) throws Exception {
		return state.getSerializedValue(key);
	}

	@Override
	public void setCurrentNamespace(Object ns) {
		state.setCurrentNamespace(ns);
		this.currentNamespace = ns;
	}

}
