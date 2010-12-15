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
		String string = Transformer.objectToString(toFlatten);
		SerializablePerson deflattened = (SerializablePerson) Transformer.stringToObject(string);
		Assert.assertEquals(toFlatten.name, deflattened.name);
		Assert.assertEquals(toFlatten.age, deflattened.age);
	}

	@Test(expected = NotSerializableException.class)
	public void testSerDeserNonserializable() throws Exception {
		TestNonserializableObject toFlatten = new TestNonserializableObject();
		toFlatten.name = "testName";
		toFlatten.age = 300;
		Transformer.objectToString(toFlatten);
	}

	static class TestNonserializableObject {
		@SuppressWarnings("unused")
		private String name;
		@SuppressWarnings("unused")
		private int age;
	}
}