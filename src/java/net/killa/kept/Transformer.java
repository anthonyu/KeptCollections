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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

final class Transformer {
    static String objectToString(final Object object, final Class<?> objectType)
	    throws IOException {
	return Transformer.bytesToString(Transformer.objectToBytes(object,
		objectType));
    }

    static Object stringToObject(final String string, final Class<?> objectType)
	    throws IOException, ClassNotFoundException {
	return Transformer.bytesToObject(Transformer.stringToBytes(string),
		objectType);
    }

    static byte[] objectToBytes(final Object object, final Class<?> objectType)
	    throws IOException {
	byte[] bytes = null;
	if (object != null) {
	    if (STRINGIFIABLE_PRIMITIVES.contains(objectType)) {
		bytes = object.toString().getBytes();
	    } else {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
		    oos = new ObjectOutputStream(baos);
		    oos.writeObject(object);
		    bytes = baos.toByteArray();
		} finally {
		    close(baos);
		    close(oos);
		}
	    }
	} else {
	    throw new IllegalArgumentException(
		    "Cannot byte-transform null object");
	}

	return bytes;
    }

    static String bytesToString(final byte[] bytes) {
	String transformed = null;
	if (bytes != null && bytes.length != 0) {
	    StringBuilder hexString = new StringBuilder(2 * bytes.length);
	    for (byte bite : bytes) {
		hexString.append("0123456789ABCDEF".charAt((bite & 0xF0) >> 4))
			.append("0123456789ABCDEF".charAt((bite & 0x0F)));
	    }
	    transformed = hexString.toString();
	} else {
	    throw new IllegalArgumentException(
		    "Cannot string-transform null or empty byte array");
	}

	return transformed;
    }

    static Object bytesToObject(final byte[] bytes, final Class<?> objectType)
	    throws IOException, ClassNotFoundException {
	Object object = null;
	if (bytes != null && bytes.length != 0) {
	    if (STRINGIFIABLE_PRIMITIVES.contains(objectType)) {
		object = transform(objectType, new String(bytes));
	    } else {
		InputStream is = null;
		ObjectInputStream ois = null;
		try {
		    is = new ByteArrayInputStream(bytes);
		    ois = new ObjectInputStream(is);
		    object = ois.readObject();
		} finally {
		    close(is);
		    close(ois);
		}
	    }
	} else {
	    throw new IllegalArgumentException(
		    "Cannot object-transform null or empty byte array");
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
		bytes[i++] = (byte) (Character.digit(upper, 16) << 4 | Character
			.digit(lower, 16));
	    }
	} else {
	    throw new IllegalArgumentException(
		    "Cannot byte-transform null or empty string");
	}

	return bytes;
    }

    private static Object transform(final Class<?> type,
	    final String toTransform) {
	if (type.equals(String.class)) {
	    return toTransform;
	}
	if (type.equals(Byte.class) || type.equals(byte.class)) {
	    return Byte.valueOf(toTransform);
	}
	if (type.equals(Character.class) || type.equals(char.class)) {
	    if (!isNullOrEmpty(toTransform)) {
		if (toTransform.length() > 1) {
		    throw new IllegalArgumentException(
			    "Non-character value masquerading as characters in a string");
		}
		return toTransform.charAt(0);
	    } else {
		return '\u0000';
	    }
	}
	if (type.equals(Short.class) || type.equals(short.class)) {
	    return Short.valueOf(toTransform);
	}
	if (type.equals(Integer.class) || type.equals(int.class)) {
	    if (toTransform == null) {
		return 0;
	    }
	    return Integer.valueOf(toTransform);
	}
	if (type.equals(Float.class) || type.equals(float.class)) {
	    if (toTransform == null) {
		return 0f;
	    }
	    return Float.valueOf(toTransform);
	}
	if (type.equals(Double.class) || type.equals(double.class)) {
	    return Double.valueOf(toTransform);
	}
	if (type.equals(Long.class) || type.equals(long.class)) {
	    return Long.valueOf(toTransform);
	}
	if (type.equals(Boolean.class) || type.equals(boolean.class)) {
	    return Boolean.valueOf(toTransform);
	}
	if (type.equals(BigDecimal.class)) {
	    return new BigDecimal(toTransform);
	}
	if (type.equals(BigInteger.class)) {
	    return new BigInteger(toTransform);
	}

	return toTransform;
    }

    private static boolean isNullOrEmpty(final Object obj) {
	if (obj == null) {
	    return true;
	}
	if (obj.getClass().equals(Collection.class)) {
	    return ((Collection<?>) obj).size() == 0;
	} else {
	    if (obj.toString().trim().length() == 0) {
		return true;
	    }
	}

	return false;
    }

    private static void close(final Closeable stream) throws IOException {
	if (stream != null) {
	    stream.close();
	}
    }

    static final Set<Class<?>> STRINGIFIABLE_PRIMITIVES = new HashSet<Class<?>>();
    static {
	STRINGIFIABLE_PRIMITIVES.add(String.class);
	STRINGIFIABLE_PRIMITIVES.add(Byte.class);
	STRINGIFIABLE_PRIMITIVES.add(byte.class);
	STRINGIFIABLE_PRIMITIVES.add(Character.class);
	STRINGIFIABLE_PRIMITIVES.add(char.class);
	STRINGIFIABLE_PRIMITIVES.add(Short.class);
	STRINGIFIABLE_PRIMITIVES.add(short.class);
	STRINGIFIABLE_PRIMITIVES.add(Integer.class);
	STRINGIFIABLE_PRIMITIVES.add(int.class);
	STRINGIFIABLE_PRIMITIVES.add(Float.class);
	STRINGIFIABLE_PRIMITIVES.add(float.class);
	STRINGIFIABLE_PRIMITIVES.add(Double.class);
	STRINGIFIABLE_PRIMITIVES.add(double.class);
	STRINGIFIABLE_PRIMITIVES.add(Long.class);
	STRINGIFIABLE_PRIMITIVES.add(long.class);
	STRINGIFIABLE_PRIMITIVES.add(Boolean.class);
	STRINGIFIABLE_PRIMITIVES.add(boolean.class);
	STRINGIFIABLE_PRIMITIVES.add(BigDecimal.class);
	STRINGIFIABLE_PRIMITIVES.add(BigInteger.class);
    }

    private Transformer() {
    }
}
