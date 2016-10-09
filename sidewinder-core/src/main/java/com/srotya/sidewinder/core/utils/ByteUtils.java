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
package com.srotya.sidewinder.core.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author ambudsharma
 *
 */
public class ByteUtils {

	public static final Charset DEF_CHARSET = Charset.forName("US-ASCII"); 

	/**
	 * @param in
	 * @return
	 */
	public static byte[] intToByteMSB(int in){
		byte[] ou = new byte[4];
		ou[0] = (byte)((in >> 24) & 0xff);
		ou[1] = (byte)((in >> 16) & 0xff);
		ou[2] = (byte)((in >> 8) & 0xff);
		ou[3] = (byte)(in & 0xff);
		return ou;
	}
	
	/**
	 * @param x
	 * @return
	 */
	public static byte[] longToBytes(long x) {
	    ByteBuffer buffer = ByteBuffer.allocate(8);
	    buffer.putLong(x);
	    return buffer.array();
	}
	
	/**
	 * @param in
	 * @return
	 */
	public static byte[] shortToByteMSB(short in){
		byte[] ou = new byte[2];
		ou[0] = (byte)((in << 8) & 0xff);
		ou[1] = (byte)((in >> 8) & 0xff);
		return ou;
	}
	
	/**
	 * @param input
	 * @return
	 */
	public static byte[] stringToBytes(String input){
		byte[] ou = new byte[input.length()];
		for(int i=0; i<ou.length; i++){
			ou[i] = (byte) input.charAt(i);
		}
		return ou;
	}
	
	/**
	 * @param in
	 * @return
	 */
	public static String intToHex(int in){
		return Integer.toOctalString(in);
	}
	/**
	 * @param in
	 * @return 
	 */
	public static String byteAryToAscii(byte[] in){
		return new String(in, DEF_CHARSET);
	}
	
}
