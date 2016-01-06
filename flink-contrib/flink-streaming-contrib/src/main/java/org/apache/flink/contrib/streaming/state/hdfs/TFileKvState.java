package org.apache.flink.contrib.streaming.state.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.DbStateBackend;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;

public class TFileKvState<K, V> implements KvState<K, V, DbStateBackend> {

	private static final byte[] TOMBSTONE = new byte[0];

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final V defaultValue;

	private K currentKey;

	private final StateCache cache;

	private final String cpParentDir;
	private final KeyScanner scanner;

	private long nextTs = 0;

	public TFileKvState(
			String cpParentDir,
			List<String> cpFiles,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue) {

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.defaultValue = defaultValue;

		this.cpParentDir = cpParentDir;
		this.cache = new StateCache(100, 50);

		List<String> sortedCpFiles = new ArrayList<>(cpFiles);
		Collections.sort(cpFiles, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Long.compare(Long.parseLong(o1), Long.parseLong(o2));
			}
		});

		this.scanner = new KeyScanner(
				Lists.transform(sortedCpFiles, new Function<String, Path>() {
					@Override
					public Path apply(String file) {
						return new Path(TFileKvState.this.cpParentDir, file);
					}
				}));
	}

	@Override
	public void setCurrentKey(K key) {
		this.currentKey = key;
	}

	@Override
	public void update(V value) throws IOException {
		try {
			cache.put(currentKey, Optional.fromNullable(value));
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public V value() throws IOException {
		try {
			// We get the value from the cache (which will automatically load it
			// from the database if necessary). If null, we return a copy of the
			// default value
			V val = cache.get(currentKey).orNull();
			return val != null ? val : copyDefault();
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public KvStateSnapshot<K, V, DbStateBackend> snapshot(long checkpointId, long timestamp)
			throws Exception {

		SortedMap<byte[], byte[]> modifiedKVs = serializeAndSort(cache.modified.entrySet());
		writeToDisk(modifiedKVs, timestamp);

		nextTs = timestamp + 1;

		return null;
	}

	private void writeToDisk(SortedMap<byte[], byte[]> modifiedKVs, long timestamp) {
		Path cpTFile = new Path(cpParentDir, String.valueOf(timestamp));

		try (CheckpointWriter writer = new CheckpointWriter(cpTFile)) {
			writer.writeSorted(modifiedKVs);
		} catch (Exception e) {
			throw new RuntimeException("Could not write checkpoint to disk.", e);
		}

		scanner.addNewLookupFile(cpTFile);
	}

	private SortedMap<byte[], byte[]> serializeAndSort(Collection<Entry<K, Optional<V>>> modified) throws IOException {
		SortedMap<byte[], byte[]> sortedKVs = new TreeMap<>(UnsignedBytes.lexicographicalComparator());

		for (Entry<K, Optional<V>> entry : modified) {
			Optional<V> val = entry.getValue();
			sortedKVs.put(
					InstantiationUtil.serializeToByteArray(keySerializer, entry.getKey()),
					val.isPresent() ? InstantiationUtil.serializeObject(val.get()) : TOMBSTONE);
		}

		return sortedKVs;
	}

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void dispose() {
		// Remove all files?
	}

	public final class StateCache extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		private final Map<K, Optional<V>> modified = new HashMap<>();

		public StateCache(int cacheSize, int evictionSize) {
			super(cacheSize, 0.75f, true);
			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;
		}

		@Override
		public Optional<V> put(K key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> old = super.put(key, value);
			modified.put(key, value);
			evictIfFull();
			return old;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			// First we check whether the value is cached
			Optional<V> value = super.get(key);
			if (value == null) {
				// Read from disk
				value = Optional.fromNullable(getFromDiskOrNull((K) key));
				put((K) key, value);
			}
			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		private V getFromDiskOrNull(final K key) {
			try {
				final byte[] serializedKey = InstantiationUtil.serializeToByteArray(keySerializer, key);

				byte[] serializedVal = scanner.lookup(serializedKey);

				return serializedVal != null && serializedVal.length > 0
						? InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal) : null;

			} catch (IOException e) {
				// We need to re-throw this exception to conform to the map
				// interface, we will catch this when we call the the put/get
				throw new RuntimeException("Could not get state for key: " + key, e);
			}

		}

		private void evictIfFull() {
			if (size() > cacheSize) {
				try {
					int numEvicted = 0;
					Iterator<Entry<K, Optional<V>>> entryIterator = entrySet().iterator();
					List<Entry<K, Optional<V>>> toEvict = new ArrayList<>();

					while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

						Entry<K, Optional<V>> next = entryIterator.next();

						// We only need to write to the database if modified
						if (modified.remove(next.getKey()) != null) {
							toEvict.add(next);
						}

						entryIterator.remove();
					}

					writeToDisk(serializeAndSort(toEvict), nextTs);

					nextTs++;

				} catch (IOException e) {
					// We need to re-throw this exception to conform to the map
					// interface, we will catch this when we call the the
					// put/get
					throw new RuntimeException("Could not evict state", e);
				}
			}
		}

		@Override
		public void putAll(Map<? extends K, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

	}

}
