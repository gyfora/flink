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

package org.apache.flink.streaming.api.state;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class OperatorStateUtils {

	public static Field[] getStateFields(Object udf) {
		Field[] fields = udf.getClass().getDeclaredFields();
		List<Field> stateFields = new ArrayList<Field>();
		for (Field field : fields) {
			if (field.getAnnotation(State.class) instanceof State) {
				stateFields.add(field);
			}
		}
		
		Field[] returnFields = new Field[stateFields.size()];
		return stateFields.toArray(returnFields);
	}

	public static void restoreStates(Object[] states, Object udf, Field[] stateFields)
			throws IllegalArgumentException, IllegalAccessException {
		for (int i = 0; i < stateFields.length; i++) {
			Field f = stateFields[i];
			f.setAccessible(true);
			f.set(udf, states[i]);
		}
	}

	public static Object[] getStates(Object udf, Field[] stateFields)
			throws IllegalArgumentException, IllegalAccessException {

		Object[] states = new Object[stateFields.length];

		for (int i = 0; i < stateFields.length; i++) {
			Field f = stateFields[i];
			f.setAccessible(true);
			states[i] = f.get(udf);
		}

		return states;
	}
}
