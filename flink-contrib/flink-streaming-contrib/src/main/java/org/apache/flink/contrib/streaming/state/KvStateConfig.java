package org.apache.flink.contrib.streaming.state;

import java.io.Serializable;

public class KvStateConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// Dedault properties
	protected int kvStateCacheSize;
	protected float maxKvEvictFraction;

	protected int bfExpectedInserts = -1;
	protected double bfFPP = -1;

	public KvStateConfig(int kvStateCacheSize, float maxCacheEvictFraction) {
		this.kvStateCacheSize = kvStateCacheSize;
		this.maxKvEvictFraction = maxCacheEvictFraction;
	}

	public KvStateConfig() {
		this(10000, 0.1f);
	}

	/**
	 * The maximum number of key-value pairs stored in one task instance's cache
	 * before evicting to the underlying storage layer.
	 *
	 */
	public int getKvCacheSize() {
		return kvStateCacheSize;
	}

	/**
	 * Set the maximum number of key-value pairs stored in one task instance's
	 * cache before evicting to the underlying storage layer. When the cache is
	 * full the N least recently used keys will be evicted to the storage, where
	 * N = maxKvEvictFraction*KvCacheSize.
	 *
	 */
	public void setKvCacheSize(int size) {
		kvStateCacheSize = size;
	}

	/**
	 * Sets the maximum fraction of key-value states evicted from the cache if
	 * the cache is full.
	 */
	public void setMaxKvCacheEvictFraction(float fraction) {
		if (fraction > 1 || fraction <= 0) {
			throw new RuntimeException("Must be a number between 0 and 1");
		} else {
			maxKvEvictFraction = fraction;
		}
	}

	/**
	 * The maximum fraction of key-value states evicted from the cache if the
	 * cache is full.
	 */
	public float getMaxKvCacheEvictFraction() {
		return maxKvEvictFraction;
	}

	/**
	 * The number of elements that will be evicted when the cache is full.
	 * 
	 */
	public int getNumElementsToEvict() {
		return (int) Math.ceil(getKvCacheSize() * getMaxKvCacheEvictFraction());
	}

	public void setBloomFilter(int expectedInsertions, double fpp) {
		this.bfExpectedInserts = expectedInsertions;
		this.bfFPP = fpp;
	}

	public boolean hasBloomFilter() {
		return bfExpectedInserts > 0;
	}

	public int getBloomFilterExpectedInserts() {
		return bfExpectedInserts;
	}

	public double getBloomFilterFPP() {
		return bfFPP;
	}

}
