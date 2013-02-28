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

import java.io.NotSerializableException;

import junit.framework.Assert;

import org.junit.Test;

public class TransformerTest {
    @Test
    public void testSerDeser() throws Exception {
	SerializablePerson toFlatten = new SerializablePerson();
	toFlatten.name = "testName";
	toFlatten.age = 300;
	String string = Transformer.objectToString(toFlatten,
		SerializablePerson.class);
	SerializablePerson deflattened = (SerializablePerson) Transformer
		.stringToObject(string, SerializablePerson.class);
	Assert.assertEquals(toFlatten.name, deflattened.name);
	Assert.assertEquals(toFlatten.age, deflattened.age);
    }

    @Test(expected = NotSerializableException.class)
    public void testSerDeserNonserializable() throws Exception {
	NonserializablePerson toFlatten = new NonserializablePerson();
	toFlatten.name = "testName";
	toFlatten.age = 300;
	Transformer.objectToString(toFlatten, NonserializablePerson.class);
    }

    @Test
    public void testSerDeserIntPerf() throws Exception {
	long startNanos = System.nanoTime();
	for (int i = 0; i < 10000; i++) {
	    Assert.assertEquals(i, Transformer.bytesToObject(Transformer
		    .objectToBytes(i, int.class), int.class));
	}
	long elapsed1 = System.nanoTime() - startNanos;

	Transformer.STRINGIFIABLE_PRIMITIVES.remove(int.class);

	startNanos = System.nanoTime();
	for (int i = 0; i < 10000; i++) {
	    Assert.assertEquals(i, Transformer.bytesToObject(Transformer
		    .objectToBytes(i, int.class), int.class));
	}
	long elapsed2 = System.nanoTime() - startNanos;
	Assert.assertTrue(elapsed2 > elapsed1);
	Assert.assertTrue(elapsed2 > 4 * elapsed1);
	System.out.println("Stringified serDeser (10k ints) nanos=" + elapsed1
		+ "\nNon-Stringified serDeser (10k ints) nanos=" + elapsed2);
    }

    @Test
    public void testSerDeserLongPerf() throws Exception {
	long startNanos = System.nanoTime();
	for (long i = 0; i < 10000L; i++) {
	    Assert.assertEquals(i, Transformer.bytesToObject(Transformer
		    .objectToBytes(i, long.class), long.class));
	}
	long elapsed1 = System.nanoTime() - startNanos;

	Transformer.STRINGIFIABLE_PRIMITIVES.remove(long.class);

	startNanos = System.nanoTime();
	for (long i = 0; i < 10000L; i++) {
	    Assert.assertEquals(i, Transformer.bytesToObject(Transformer
		    .objectToBytes(i, long.class), long.class));
	}
	long elapsed2 = System.nanoTime() - startNanos;
	Assert.assertTrue("stringifiable is slower than serializable", elapsed2 > elapsed1);
	Assert.assertTrue("stringifiable is not four times faster than serializable", elapsed2 > 4 * elapsed1);
	System.out.println("Stringified serDeser (10k longs) nanos=" + elapsed1
		+ "\nNon-Stringified serDeser (10k longs) nanos=" + elapsed2);
    }

    @Test
    public void testSerDeserStringPerf() throws Exception {
	long startNanos = System.nanoTime();
	for (long i = 0; i < 10000L; i++) {
	    Assert.assertEquals("String" + i, Transformer.bytesToObject(
		    Transformer.objectToBytes("String" + i, String.class),
		    String.class));
	}
	long elapsed1 = System.nanoTime() - startNanos;

	Transformer.STRINGIFIABLE_PRIMITIVES.remove(String.class);

	startNanos = System.nanoTime();
	for (long i = 0; i < 10000L; i++) {
	    Assert.assertEquals("String" + i, Transformer.bytesToObject(
		    Transformer.objectToBytes("String" + i, String.class),
		    String.class));
	}
	long elapsed2 = System.nanoTime() - startNanos;
	Assert.assertTrue("stringifiable is slower than serializable", elapsed2 > elapsed1);
	System.out.println("Stringified serDeser (10k strings) nanos="
		+ elapsed1 + "\nNon-Stringified serDeser (10k strings) nanos="
		+ elapsed2);
    }
}