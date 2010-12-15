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
package net.killa.kept;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

final class Transformer {
	static String objectToString(final Object object) throws IOException {
		return Transformer.bytesToString(Transformer.objectToBytes(object));
	}

	static Object stringToObject(final String string) throws IOException, ClassNotFoundException {
		return Transformer.bytesToObject(Transformer.stringToBytes(string));
	}

	static byte[] objectToBytes(final Object object) throws IOException {
		byte[] bytes = null;
		if (object != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(baos);
				oos.writeObject(object);
				bytes = baos.toByteArray();
			} finally {
				baos.close();
				oos.close();
			}
		} else {
			throw new IllegalArgumentException("Cannot byte-transform null object");
		}

		return bytes;
	}

	static String bytesToString(final byte[] bytes) {
		String transformed = null;
		if (bytes != null && bytes.length != 0) {
			StringBuilder hexString = new StringBuilder(2 * bytes.length);
			for (byte bite : bytes) {
				hexString.append("0123456789ABCDEF".charAt((bite & 0xF0) >> 4)).append("0123456789ABCDEF".charAt((bite & 0x0F)));
			}
			transformed = hexString.toString();
		} else {
			throw new IllegalArgumentException("Cannot string-transform null or empty byte array");
		}

		return transformed;
	}

	static Object bytesToObject(final byte[] bytes) throws IOException, ClassNotFoundException {
		Object object = null;
		if (bytes != null && bytes.length != 0) {
			InputStream is = null;
			ObjectInputStream ois = null;
			try {
				is = new ByteArrayInputStream(bytes);
				ois = new ObjectInputStream(is);
				object = ois.readObject();
			} finally {
				is.close();
				ois.close();
			}
		} else {
			throw new IllegalArgumentException("Cannot object-transform null or empty byte array");
		}

		return object;
	}

	static byte[] stringToBytes(final String string) {
		byte[] bytes = null;
		if (string != null && string.length() != 0) {
			bytes = new byte[string.length() / 2];
			for (int i = 0, j = 0; i < bytes.length;) {
				char upper = Character.toLowerCase(string.charAt(j++));
				char lower = Character.toLowerCase(string.charAt(j++));
				bytes[i++] = (byte) (Character.digit(upper, 16) << 4 | Character.digit(lower, 16));
			}
		} else {
			throw new IllegalArgumentException("Cannot byte-transform null or empty string");
		}

		return bytes;
	}

	private Transformer() {
	}
}
