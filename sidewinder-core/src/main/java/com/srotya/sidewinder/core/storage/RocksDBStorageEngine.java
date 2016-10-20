/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.srotya.sidewinder.core.utils.ByteUtils;

import io.symcpe.wraith.MurmurHash;

/**
 * @author ambudsharma
 */
public class RocksDBStorageEngine extends AbstractStorageEngine {

	private static final Charset CHARSET = Charset.forName("utf-8");
	private RocksDB indexdb;
	private Options indexdbOptions;
	private RocksDB tsdb;
	private Options tsdbOptions;
	private WriteOptions writeOptions;
	private String tsdbWalDirectory;
	private String tsdbMemDirectory;
	private String indexdbMemDirectory;
	private String indexdbWalDirectory;
	private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	static {
		RocksDB.loadLibrary();
	}

	@SuppressWarnings("resource")
	@Override
	public void configure(Map<String, String> conf) throws IOException {
		tsdbWalDirectory = conf.getOrDefault("tsdb.wal.directory", "target/tsdbw");
		tsdbMemDirectory = conf.getOrDefault("tsdb.mem.directory", "target/tsdbm");
		
		indexdbWalDirectory = conf.getOrDefault("idxdb.wal.directory", "target/idxdbw");
		indexdbMemDirectory = conf.getOrDefault("idxdb.mem.directory", "target/idxdbm");
		
		if (true) {
			wipeDirectory(tsdbWalDirectory);
			wipeDirectory(tsdbMemDirectory);
			wipeDirectory(indexdbWalDirectory);
			wipeDirectory(indexdbMemDirectory);
		}
		tsdbOptions = new Options().setCreateIfMissing(true).setAllowMmapReads(true).setAllowMmapWrites(true)
				.setIncreaseParallelism(2).setFilterDeletes(true).setMaxBackgroundCompactions(10)
				.setMaxBackgroundFlushes(10).setDisableDataSync(false).setUseFsync(false).setUseAdaptiveMutex(false)
				.setWriteBufferSize(1 * SizeUnit.MB).setCompactionStyle(CompactionStyle.UNIVERSAL)
				.setCompressionType(CompressionType.SNAPPY_COMPRESSION).setMaxWriteBufferNumber(6).setWalTtlSeconds(60)
				.setWalSizeLimitMB(512).setMaxTotalWalSize(1024 * SizeUnit.MB).setErrorIfExists(false)
				.setAllowOsBuffer(true).setWalDir(tsdbWalDirectory).setOptimizeFiltersForHits(false);

		indexdbOptions = new Options().setCreateIfMissing(true).setAllowMmapReads(true).setAllowMmapWrites(true)
				.setIncreaseParallelism(2).setFilterDeletes(true).setMaxBackgroundCompactions(10)
				.setMaxBackgroundFlushes(10).setDisableDataSync(false).setUseFsync(false).setUseAdaptiveMutex(false)
				.setWriteBufferSize(1 * SizeUnit.MB).setCompactionStyle(CompactionStyle.UNIVERSAL)
				.setCompressionType(CompressionType.SNAPPY_COMPRESSION).setMaxWriteBufferNumber(6).setWalTtlSeconds(60)
				.setWalSizeLimitMB(512).setMaxTotalWalSize(1024 * SizeUnit.MB).setErrorIfExists(false)
				.setAllowOsBuffer(true).setWalDir(indexdbWalDirectory).setOptimizeFiltersForHits(false);
		writeOptions = new WriteOptions().setDisableWAL(false).setSync(false);
	}

	private void wipeDirectory(String directory) {
		File file = new File(directory);
		if (file.isDirectory() && file.exists()) {
			Arrays.asList(file.listFiles()).forEach((f) -> {
				f.delete();
			});
			file.delete();
			file.mkdirs();
		}
	}
	
	@Override
	public void connect() throws IOException {
		try {
			tsdb = RocksDB.open(tsdbOptions, tsdbMemDirectory);
			indexdb = RocksDB.open(indexdbOptions, indexdbMemDirectory);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void disconnect() throws IOException {
		if (tsdb != null) {
			tsdb.close();
		}
		if (indexdb != null) {
			indexdb.close();
		}
		if (writeOptions != null) {
			writeOptions.close();
		}
		if (indexdbOptions != null) {
			indexdbOptions.close();
		}
		if (tsdbOptions != null) {
			tsdbOptions.close();
		}
	}

	@Override
	public byte[] indexIdentifier(String identifier) throws IOException {
		try {
			byte[] key = identifier.getBytes(CHARSET);
			byte[] val = indexdb.get(key);
			if (val == null) {
				val = ByteUtils.intToByteMSBTruncated(MurmurHash.hash32(identifier));
				indexdb.put(key, val);
			}
			return val;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeSeriesPoint(byte[] rowKey, long timestamp, byte[] value) throws IOException {
		String encodedKey = Base64.getEncoder().encodeToString(rowKey);
		synchronized (encodedKey.intern()) {
			try {
				byte[] ds = tsdb.get(rowKey);
				TreeMap<Long, byte[]> map;
				if (ds != null) {
					map = kryoThreadLocal.get().readObject(new Input(ds), TreeMap.class);
				} else {
					map = new TreeMap<>();
					ds = new byte[0];
				}
				ByteArrayOutputStream stream = new ByteArrayOutputStream(ds.length + 20);
				Output output = new Output(stream);
				kryoThreadLocal.get().writeObject(output, map);
				output.close();
				tsdb.put(rowKey, stream.toByteArray());
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}
	}

}