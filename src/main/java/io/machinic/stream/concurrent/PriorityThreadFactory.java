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

package io.machinic.stream.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility {@code ThreadFactory} implementation that allows setting {@code Thread} name and priority.
 */
public class PriorityThreadFactory implements ThreadFactory {

    private final AtomicLong counter = new AtomicLong();
    private final String namePrefix;
    private final int priority;

    public PriorityThreadFactory(String namePrefix, int priority) {
        this.namePrefix = namePrefix;
        this.priority = priority;
    }
    
    /**
     * Creates the name for the next thread
     * @return next thread name
     */
    protected String nextName() {
        return String.format("%s-%s", this.namePrefix, this.counter.getAndIncrement());
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return Thread.ofPlatform()
                .name(this.nextName())
                .priority(this.priority)
                .daemon(true)
                .unstarted(runnable);
    }
}
