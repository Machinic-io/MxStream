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

package io.machinic.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MxStreamExceptionHandler {
	
	public void onException(Exception e, Object value) throws StreamException;
	
	public static class DefaultMxStreamExceptionHandler implements MxStreamExceptionHandler {
		private static final Logger logger = LoggerFactory.getLogger(DefaultMxStreamExceptionHandler.class);
		
		@Override
		public void onException(Exception e, Object value) throws StreamException {
			if (e instanceof StreamEventException) {
				//noinspection LogStatementNotGuardedByLogCondition
				logger.info(String.format("Ignoring StreamEventException: %s", e.getMessage()), e);
				return;
			}
			if (e instanceof StreamException) {
				throw (StreamException) e;
			}
			throw new StreamException(String.format("Stream failed with unhandled exception: %s", e.getMessage()), e);
		}
	}
}
