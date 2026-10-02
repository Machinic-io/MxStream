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

import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface MxStreamFunction<T, R> {
    
    R apply(T t) throws InterruptedException, StreamException;


    default <V> MxStreamFunction<V, R> compose(MxStreamFunction<? super V, ? extends T> before) throws InterruptedException, StreamException {
        Objects.requireNonNull(before);
        return (V v) -> apply(before.apply(v));
    }
    
    default <V> MxStreamFunction<V, R> compose(Function<? super V, ? extends T> before) throws InterruptedException, StreamException {
        Objects.requireNonNull(before);
        return (V v) -> apply(before.apply(v));
    }
    
    default <V> MxStreamFunction<T, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }

    default <V> MxStreamFunction<T, V> andThen(MxStreamFunction<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }
 
    static <T> MxStreamFunction<T, T> identity() {
        return t -> t;
    }
}
