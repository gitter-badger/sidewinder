/**
 * Copyright 2016 Michael Burman
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
package com.srotya.sidewinder.gorillac;

import com.srotya.sidewinder.gorillac.ByteBufferBitInput;
import com.srotya.sidewinder.gorillac.ByteBufferBitOutput;
import com.srotya.sidewinder.gorillac.Compressor;
import com.srotya.sidewinder.gorillac.Decompressor;
import com.srotya.sidewinder.gorillac.Pair;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

/**
 * These are generic tests to test that input matches the output after
 * compression + decompression cycle, using both the timestamp and value
 * compression.
 *
 * @author Michael Burman
 */
public class EncodeTest {

	@Test
	public void simpleEncodeAndDecodeTest() throws Exception {
		long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();

		ByteBufferBitOutput output = new ByteBufferBitOutput();

		Compressor c = new Compressor(now, output);

		Pair[] pairs = { new Pair(now + 10, Double.doubleToRawLongBits(1.0)),
				new Pair(now + 20, Double.doubleToRawLongBits(-2.0)),
				new Pair(now + 28, Double.doubleToRawLongBits(-2.5)),
				new Pair(now + 84, Double.doubleToRawLongBits(65537)),
				new Pair(now + 400, Double.doubleToRawLongBits(2147483650.0)),
				new Pair(now + 2300, Double.doubleToRawLongBits(-16384)),
				new Pair(now + 16384, Double.doubleToRawLongBits(2.8)),
				new Pair(now + 16500, Double.doubleToRawLongBits(-38.0)) };

		Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getDoubleValue()));
		c.close();

		ByteBuffer byteBuffer = output.getByteBuffer();
		byteBuffer.flip();

		ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		Decompressor d = new Decompressor(input, pairs.length);

		// Replace with stream once decompressor supports it
		for (int i = 0; i < pairs.length; i++) {
			Pair pair = d.readPair();
			assertEquals("Timestamp did not match", pairs[i].getTimestamp(), pair.getTimestamp());
			assertEquals("Value did not match", pairs[i].getDoubleValue(), pair.getDoubleValue(), 0);
		}

		assertNull(d.readPair());
	}

	/**
	 * Tests encoding of similar floats, see
	 * https://github.com/dgryski/go-tsz/issues/4 for more information.
	 */
	@Test
	public void testEncodeSimilarFloats() throws Exception {
		long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

		ByteBufferBitOutput output = new ByteBufferBitOutput();
		Compressor c = new Compressor(now, output);

		ByteBuffer bb = ByteBuffer.allocate(5 * 2 * Long.BYTES);

		bb.putLong(now + 1);
		bb.putDouble(6.00065e+06);
		bb.putLong(now + 2);
		bb.putDouble(6.000656e+06);
		bb.putLong(now + 3);
		bb.putDouble(6.000657e+06);
		bb.putLong(now + 4);
		bb.putDouble(6.000659e+06);
		bb.putLong(now + 5);
		bb.putDouble(6.000661e+06);

		bb.flip();

		for (int j = 0; j < 5; j++) {
			c.addValue(bb.getLong(), bb.getDouble());
		}

		c.close();

		bb.flip();

		ByteBuffer byteBuffer = output.getByteBuffer();
		byteBuffer.flip();

		ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		Decompressor d = new Decompressor(input, 5);

		// Replace with stream once decompressor supports it
		for (int i = 0; i < 5; i++) {
			Pair pair = d.readPair();
			assertEquals("Timestamp did not match", bb.getLong(), pair.getTimestamp());
			assertEquals("Value did not match", bb.getDouble(), pair.getDoubleValue(), 0);
		}
		assertNull(d.readPair());
	}

	/**
	 * Tests writing enough large amount of datapoints that causes the included
	 * ByteBufferBitOutput to do internal byte array expansion.
	 */
	@Test
	public void testEncodeLargeAmountOfData() throws Exception {
		// This test should trigger ByteBuffer reallocation
		int amountOfPoints = 100000;
		long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();
		ByteBufferBitOutput output = new ByteBufferBitOutput();

		long now = blockStart + 60;
		ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2 * Long.BYTES);

		for (int i = 0; i < amountOfPoints; i++) {
			bb.putLong(now + i * 60);
			bb.putDouble(i * Math.random());
		}

		Compressor c = new Compressor(blockStart, output);

		bb.flip();

		for (int j = 0; j < amountOfPoints; j++) {
			c.addValue(bb.getLong(), bb.getDouble());
		}

		c.close();

		bb.flip();

		ByteBuffer byteBuffer = output.getByteBuffer();
		byteBuffer.flip();

		ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		Decompressor d = new Decompressor(input, amountOfPoints);

		for (int i = 0; i < amountOfPoints; i++) {
			long tStamp = bb.getLong();
			double val = bb.getDouble();
			Pair pair = d.readPair();
			assertEquals("Expected timestamp did not match at point " + i, tStamp, pair.getTimestamp());
			assertEquals(val, pair.getDoubleValue(), 0);
		}
		assertNull(d.readPair());
	}

	/**
	 * Although not intended usage, an empty block should not cause errors
	 */
	@Test
	public void testEmptyBlock() throws Exception {
		long now = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();

		ByteBufferBitOutput output = new ByteBufferBitOutput();

		Compressor c = new Compressor(now, output);
		c.close();

		ByteBuffer byteBuffer = output.getByteBuffer();
		byteBuffer.flip();

		ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		Decompressor d = new Decompressor(input, 0);

		assertNull(d.readPair());
	}

	/**
	 * Long values should be compressable and decompressable in the stream
	 */
	@Test
	public void testLongEncoding() throws Exception {
		// This test should trigger ByteBuffer reallocation
		int amountOfPoints = 10000;
		long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();
		ByteBufferBitOutput output = new ByteBufferBitOutput();

		long now = blockStart + 60;
		ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2 * Long.BYTES);

		for (int i = 0; i < amountOfPoints; i++) {
			bb.putLong(now + i * 60);
			bb.putLong(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE));
		}

		Compressor c = new Compressor(blockStart, output);

		bb.flip();

		for (int j = 0; j < amountOfPoints; j++) {
			c.addValue(bb.getLong(), bb.getLong());
		}

		c.close();

		bb.flip();

		ByteBuffer byteBuffer = output.getByteBuffer();
		byteBuffer.flip();

		ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		Decompressor d = new Decompressor(input, amountOfPoints);

		for (int i = 0; i < amountOfPoints; i++) {
			long tStamp = bb.getLong();
			long val = bb.getLong();
			Pair pair = d.readPair();
			assertEquals("Expected timestamp did not match at point " + i, tStamp, pair.getTimestamp());
			assertEquals(val, pair.getLongValue());
		}
		assertNull(d.readPair());
	}
}
