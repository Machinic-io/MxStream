/*
 * Copyright 2026 Machinic.io LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.machinic.stream.util;

public class Require {
	
	public static void equalOrGreater(int value, int required, String fieldName) {
		if (!(value >= required)) {
			throw new IllegalArgumentException(String.format("%s %s must be rather than or equal to %s", fieldName, value, required));
		}
	}
	
	public static void equalOrGreater(long value, long required, String fieldName) {
		if (!(value >= required)) {
			throw new IllegalArgumentException(String.format("%s %s must be rather than or equal to %s", fieldName, value, required));
		}
	}
}
