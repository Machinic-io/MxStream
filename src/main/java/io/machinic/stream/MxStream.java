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

import io.machinic.stream.metrics.AsyncMapMetricSupplier;
import io.machinic.stream.metrics.StreamMetricSupplier;
import io.machinic.stream.util.Require;

import java.io.BufferedReader;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * The MxStream interface represents a custom stream with additional functionalities beyond the standard Java streams. It includes methods for parallel processing, asynchronous mapping, batching, filtering, sorting and more. The interface also supports
 * exception handling, and conversions from standard Java streams.
 * <ul>
 *   <li>Operations accepting suppliers will call supplier.get for each partition when running as a parallel stream.</li>
 *   <li>Operations are performed per partition unless otherwise noted.</li>
 * </ul>
 * @param <T>
 */
public interface MxStream<T> {
	
	/**
	 * Indicates if this is a parallel stream. This method can be used to determine whether the stream will be processed concurrently by multiple threads.
	 * @return true if the stream is parallel, false otherwise
	 */
	boolean isParallel();
	
	/**
	 * Returns the parallelism level of the stream. The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @return the parallelism level
	 */
	int getParallelism();
	
	/**
	 * Returns the default timeout in milliseconds for an asynchronous operation on this stream.
	 * @return The asynchronous timeout in milliseconds.
	 */
	long getAsyncTimeoutMillis();
	
	/**
	 * Sets the default timeout in milliseconds for an asynchronous operation on this stream. This method can be used to configure the maximum amount of time that an asynchronous operation will take before it is canceled.
	 * @param timeoutMillis The default timeout in milliseconds
	 */
	MxStream<T> asyncTimeoutMillis(long timeoutMillis);
	
	/**
	 * Indicates if the stream is closed. This method can be used to check whether the stream has reached its end or has been manually stopped.
	 * @return true if the stream is closed, false otherwise
	 */
	boolean isClosed();
	
	/**
	 * Returns the exception encountered during stream processing, if any. If an error occurs while processing a stream, this method will return the corresponding exception.
	 * @return the exception encountered, or null if no exception occurred
	 */
	StreamException getException();
	
	/**
	 * Sets the exception handler for the stream. This method can be used to configure how exceptions are handled when they occur during stream processing.
	 * @param exceptionHandler the exception handler
	 * @return a new stream with the mapped elements
	 */
	MxStream<T> exceptionHandler(MxStreamExceptionHandler exceptionHandler);
	
	/**
	 * Returns the exception handler for the stream. This method can be used to check the current exception handling configuration of the stream.
	 * @return the exception handler
	 */
	MxStreamExceptionHandler exceptionHandler();
	
	/**
	 * Begin gracefully stopping this stream. This will be done by shutting down the source and allowing events in the stream to complete processing. Note that this method will not cancel any ongoing operations but rather simply stop new events from being
	 * added to the stream.
	 */
	void stop();
	
	/**
	 * Closes the stream and handles any necessary cleanup to release resources. Processing will be aborted which can result in a non-deterministic state. Note that this method is not the same as stopping the stream, but rather explicitly terminating the
	 * stream and releasing its resources.
	 * @throws Exception if an error occurs during closure
	 */
	void close() throws Exception;
	
	/**
	 * Filters the elements of the stream using the given predicate. This method can be used to remove certain elements from the stream based on their properties or values.
	 * @param predicate the predicate to apply to each element
	 * @return a new stream with the mapped elements
	 */
	default MxStream<T> filter(Predicate<? super T> predicate) {
		Objects.requireNonNull(predicate);
		return this.filter(() -> predicate);
	}
	
	/**
	 * Filters the elements of the stream using the predicate provided by the supplier. This method is similar to `filter` but uses a separate function to generate the predicate, allowing for more flexibility in filtering logic.
	 * @param supplier the supplier providing the predicate
	 * @return a new stream with the mapped elements
	 */
	MxStream<T> filter(Supplier<Predicate<? super T>> supplier);
	
	/**
	 * Skips the fist n elements of the stream. This method can be used to remove certain initial elements from the stream, allowing for more control over processing order.
	 * @param n the number of elements to skip
	 * @return a new stream with the mapped elements
	 */
	MxStream<T> skip(long n);
	
	/**
	 * Limits the number of elements returned by the stream to the specified value. This method can be used to truncate the stream at a certain point, allowing for more control over processing size.
	 * @param n the maximum number of elements to return
	 * @return a new MxStream containing at most n occurrences of type T
	 */
	MxStream<T> limit(long n);
	
	/**
	 * Capture metrics from this point in stream. This method can be used to track performance and other metrics for processing operations.
	 * @param streamMetricSupplier the metrics supplier that will be used to collect stream metrics
	 * @return a new stream with the mapped elements
	 */
	MxStream<T> metrics(StreamMetricSupplier streamMetricSupplier);
	
	/**
	 * Maps the elements of the stream using the given function.
	 * @param mapper the function to apply to each element
	 * @param <R> the type of the result elements
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> map(Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.map(() -> mapper);
	}
	
	/**
	 * Maps the elements of the stream using the function provided by teh supplier.
	 * @param supplier the supplier providing the function
	 * @param <R> the type of the result elements
	 * @return a new stream with the mapped elements
	 */
	<R> MxStream<R> map(Supplier<Function<? super T, ? extends R>> supplier);
	
	/**
	 * Flat maps the elements of the stream using the given function.
	 * @param mapper the function to apply to each element
	 * @param <R> the type of the result elements
	 * @return a new stream with the flat-mapped elements
	 */
	default <R> MxStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
		Objects.requireNonNull(mapper);
		return this.flatMapProducer(() -> FlatMapProducerFunction.wrap(mapper));
	}
	
	/**
	 * Flat maps the elements of the stream using the function provided by the supplier.
	 * @param supplier the supplier providing the function
	 * @param <R> the type of the result elements
	 * @return a new stream with the flat-mapped elements
	 */
	<R> MxStream<R> flatMap(Supplier<Function<? super T, ? extends Stream<? extends R>>> supplier);
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * <br/>
	 * This is a very simple example of what an implementation could look like.
	 * <pre>
	 * {@code
	 * .flatMapProducer((value, consumer) -> {
	 * 	  // Do something before stream
	 * 	  try (Stream<String> stream = Stream.of(value.split(", ?"))) {
	 * 	    stream.forEachOrdered(consumer);
	 *    } finally {
	 * 	    // do something after stream
	 *    }
	 *  })
	 * }
	 * </pre>
	 * @param <R> The type of the elements in the output stream.
	 * @param mapper A {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> flatMapProducer(FlatMapProducerFunction<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.flatMapProducer(() -> mapper);
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * <br/>
	 * This is a very simple example of what an implementation could look like.
	 * <pre>
	 * {@code
	 * .flatMapProducer(() -> (value, consumer) -> {
	 * 	  // Do something before stream
	 * 	  try (Stream<String> stream = Stream.of(value.split(", ?"))) {
	 * 	    stream.forEachOrdered(consumer);
	 *    } finally {
	 * 	    // do something after stream
	 *    }
	 *  })
	 * }
	 * </pre>
	 * @param <R> The type of the elements in the output stream.
	 * @param supplier A supplier that provides a {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	<R> MxStream<R> flatMapProducer(Supplier<FlatMapProducerFunction<? super T, ? extends R>> supplier);
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param function A {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, FlatMapProducerFunction<? super T, ? extends R> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, this.getAsyncTimeoutMillis(), null, null, () -> function);
	}
	
	/**
	 * Applies a flat map operation asynchronously with the specified parallelism and buffer size, using a function to transform each element into a stream, and returns a new MxStream
	 * @param parallelism The number of concurrent tasks to run.
	 * @param bufferSize The size of the buffer used for collecting results from parallel tasks.
	 * @param function A Function that takes an element of type T and returns a Stream of type R.
	 * @return A new MxStream resulting from applying the flat map operation asynchronously.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, Function<? super T, ? extends Stream<? extends R>> function) {
		return this.asyncFlatMapProducer(parallelism, bufferSize, this.getAsyncTimeoutMillis(), null, null, FlatMapProducerFunction.wrap(function));
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param supplier A supplier that provides a {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, Supplier<FlatMapProducerFunction<? super T, ? extends R>> supplier) {
		return this.asyncFlatMapProducer(parallelism, bufferSize, this.getAsyncTimeoutMillis(), null, null, supplier);
	}
	
	/**
	 * Applies a flat mapping function to each element of the stream asynchronously and returns a new stream with the results.
	 * @param parallelism The number of parallel threads to use for processing elements.
	 * @param bufferSize The size of the buffer used for collecting results from parallel streams.
	 * @param supplier A supplier that provides a function to apply to each element of the stream.
	 * @return A new stream with the results after applying the flat mapping function asynchronously.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, Supplier<Function<? super T, ? extends Stream<? extends R>>> supplier) {
		Objects.requireNonNull(supplier);
		return this.asyncFlatMapProducer(parallelism, bufferSize, this.getAsyncTimeoutMillis(), null, null, () -> {
			Function<? super T, ? extends Stream<? extends R>> function = supplier.get();
			if (function == null) {
				throw new IllegalArgumentException("The provided supplier must not be null.");
			}
			return FlatMapProducerFunction.wrap(function);
		});
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param function A {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, FlatMapProducerFunction<? super T, ? extends R> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, null, null, () -> function);
	}
	
	/**
	 * Applies a function to each element of the stream and asynchronously maps it to a Stream of another type, then flattens the resulting streams into a single stream.
	 * @param parallelism The number of concurrent tasks to run.
	 * @param bufferSize The size of the buffer used for collecting results from asynchronous operations.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) to wait for an asynchronous operation to complete before timing out.
	 * @param function A Function that takes an element of the current stream and returns a Stream of another type.
	 * @return A new MxStream instance containing elements of the resulting streams from applying the function.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, Function<? super T, ? extends Stream<? extends R>> function) {
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, null, null, FlatMapProducerFunction.wrap(function));
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param executorService An executor service used to run the flatMap tasks asynchronously.
	 * @param function A {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, FlatMapProducerFunction<? super T, ? extends R> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, null, () -> function);
	}
	
	/**
	 * Applies a function to each element of the stream and returns a new stream resulting from applying the provided {@code Function} to each element. The operation is performed asynchronously with the specified parallelism, buffer size, timeout, and
	 * executor service.
	 * @param parallelism the number of concurrent threads to use for processing elements.
	 * @param bufferSize the maximum number of elements that can be buffered before forcing a computation.
	 * @param asyncTimeoutMillis the maximum time to wait for an asynchronous operation to complete before timing out.
	 * @param executorService the executor service to use for executing tasks in parallel.
	 * @param function the function to apply to each element of the stream, producing a new stream of elements.
	 * @return a new {@code MxStream} that represents the result of applying the provided function to each element of this stream asynchronously.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, Function<? super T, ? extends Stream<? extends R>> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, null, FlatMapProducerFunction.wrap(function));
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param supplier A supplier that provides a {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, Supplier<FlatMapProducerFunction<? super T, ? extends R>> supplier) {
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, null, null, supplier);
	}
	
	/**
	 * Applies a flat mapping operation asynchronously with the specified parallelism, buffer size, and timeout. The function provided by the supplier is used to map each element of this stream to a {@code Stream<R>}, which is then flattened into a single
	 * stream. The mapping process is performed in parallel if specified.
	 * @param parallelism the degree of parallelism for async processing, must be greater than zero.
	 * @param bufferSize the size of the buffer used to collect elements during async processing, must be greater than zero.
	 * @param asyncTimeoutMillis the maximum time to wait before considering an asynchronous operation as failed, in milliseconds.
	 * @param supplier a supplier that provides a function to apply to each element of this stream to produce a {@code Stream<R>}. The supplier must not be null and its provided function must also not be null.
	 * @return a new {@code MxStream<R>} resulting from the application of the flat mapping operation.
	 * @throws NullPointerException if the supplier is null.
	 * @throws IllegalArgumentException if the parallelism, buffer size, or asyncTimeoutMillis are less than or equal to zero, or if the provided function by the supplier is null.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, Supplier<Function<? super T, ? extends Stream<? extends R>>> supplier) {
		Objects.requireNonNull(supplier);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, null, null, () -> {
			Function<? super T, ? extends Stream<? extends R>> function = supplier.get();
			if (function == null) {
				throw new IllegalArgumentException("The provided supplier must not be null.");
			}
			return FlatMapProducerFunction.wrap(function);
		});
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param executorService An executor service used to run the flatMap tasks asynchronously.
	 * @param supplier A supplier that provides a {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, Supplier<FlatMapProducerFunction<? super T, ? extends R>> supplier) {
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, null, supplier);
	}
	
	/**
	 * Initiates an asynchronous flat map operation with the specified parallelism, buffer size, and timeout.
	 * @param parallelism The number of parallel streams to execute.
	 * @param bufferSize The size of the buffer used for storing elements during the async operation.
	 * @param asyncTimeoutMillis The maximum time in milliseconds to wait for each asynchronous operation to complete before timing out.
	 * @param executorService The ExecutorService to use for executing the flat map operations asynchronously.
	 * @param supplier A Supplier that provides a Function to be applied to each element of the stream, which returns a Stream. The result is then flattened into the resulting stream.
	 * @return A new MxStream representing the result of the async flat map operation.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, Supplier<Function<? super T, ? extends Stream<? extends R>>> supplier) {
		Objects.requireNonNull(supplier);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, null, () -> {
			Function<? super T, ? extends Stream<? extends R>> function = supplier.get();
			if (function == null) {
				throw new IllegalArgumentException("The provided supplier must not be null.");
			}
			return FlatMapProducerFunction.wrap(function);
		});
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param executorService An executor service used to run the flatMap tasks asynchronously.
	 * @param metricSupplier A supplier that provides an {@link io.machinic.stream.metrics.AsyncMapMetric} that can be used to collect metrics from the stream.
	 * @param function A {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	default <R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, FlatMapProducerFunction<? super T, ? extends R> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, metricSupplier, () -> function);
	}
	
	/**
	 * Applies a asynchronous flat map operation to the elements of this stream using the provided function and configuration parameters.
	 * @param parallelism The number of parallel threads to use for processing.
	 * @param bufferSize The size of the buffer used for storing intermediate results.
	 * @param asyncTimeoutMillis The timeout duration in milliseconds for each asynchronous operation.
	 * @param executorService The executor service to be used for executing the flat map operations asynchronously.
	 * @param metricSupplier A supplier function for creating metric objects that can track and report performance metrics of the asyncFlatMap operation.
	 * @param function A function that takes an element from this stream and returns a Stream of another type, which will then be flattened into the resulting stream.
	 * @return A new MxStream instance containing the results after applying the asynchronous flat map operation.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Function<? super T, ? extends Stream<? extends R>> function) {
		Objects.requireNonNull(function);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, metricSupplier, FlatMapProducerFunction.wrap(function));
	}
	
	/**
	 * Converts a single element into zero or more elements like the flatMap method but gives the caller much more control over the process. This method is preferable in situations where the stream needs additional considerations, such as opening a
	 * transaction.
	 * @param parallelism The number of tasks to process at one time. Higher values result in more simultaneous processing but may consume more resources.
	 * @param bufferSize The size of the buffer used for intermediate results.
	 * @param asyncTimeoutMillis The default timeout in milliseconds for an asynchronous operation on this stream.
	 * @param executorService An executor service used to run the flatMap tasks asynchronously.
	 * @param metricSupplier A supplier that provides an {@link io.machinic.stream.metrics.AsyncMapMetric} that can be used to collect metrics from the stream.
	 * @param supplier A supplier that provides a {@link io.machinic.stream.FlatMapProducerFunction} that takes an element, converts it to a stream, and calls the consumer for each.
	 * @return A pipeline that can be used to process data from an input stream.
	 */
	<R> MxStream<R> asyncFlatMapProducer(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Supplier<FlatMapProducerFunction<? super T, ? extends R>> supplier);
	
	/**
	 * Applies a flat map operation to the elements of this stream asynchronously with specified parallelism, buffer size, and timeout.
	 * @param parallelism the number of threads to use for parallel processing (must be greater than or equal to 1).
	 * @param bufferSize the size of the buffer used for storing intermediate results (must be greater than or equal to 1).
	 * @param asyncTimeoutMillis the timeout duration in milliseconds for each asynchronous operation (must be greater than or equal to 1).
	 * @param executorService the ExecutorService to use for executing tasks.
	 * @param metricSupplier a supplier for creating AsyncMapMetric instances to track metrics during the flat map operation.
	 * @param supplier a supplier of functions that will be applied to each element of this stream to produce a new stream of elements (must not be null).
	 * @return a new MxStream resulting from applying the async flat map operation.
	 */
	default <R> MxStream<R> asyncFlatMap(int parallelism, int bufferSize, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Supplier<Function<? super T, ? extends Stream<? extends R>>> supplier) {
		Objects.requireNonNull(supplier);
		Require.equalOrGreater(parallelism, 1, "parallelism");
		Require.equalOrGreater(bufferSize, 1, "bufferSize");
		Require.equalOrGreater(asyncTimeoutMillis, 1, "asyncTimeoutMillis");
		
		Objects.requireNonNull(supplier);
		return this.asyncFlatMapProducer(parallelism, bufferSize, asyncTimeoutMillis, executorService, metricSupplier, () -> {
			Function<? super T, ? extends Stream<? extends R>> function = supplier.get();
			if (function == null) {
				throw new IllegalArgumentException("The provided supplier must not be null.");
			}
			return FlatMapProducerFunction.wrap(function);
		});
	}
	
	/**
	 * Runs the map operation asynchronously using the function provided and the default ExecutorService. The asyncMap operation will maintain stream order.
	 * @param parallelism number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param mapper the function to apply to each element
	 * @param <R> the type of the result elements
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.asyncMap(parallelism, null, mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function provided by the supplier and the default ExecutorService. The asyncMap operation will maintain stream order.
	 * @param <R> the type of objects returned by the mapper
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param mapper a function that maps each element in the stream to an object of type R, or throws an exception if the mapping fails
	 * @return a new stream containing the mapped values
	 */
	default <R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, Function<? super T, ? extends R> mapper) {
		return this.asyncMap(parallelism, asyncTimeoutMillis, null, null, () -> mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param parallelism number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param mapper the function to apply to each element
	 * @param <R> the type of the result elements
	 * @param executorService the executor service to submit tasks to
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, ExecutorService executorService, Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.asyncMap(parallelism, this.getAsyncTimeoutMillis(), executorService, null, () -> mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param <R> the type of objects returned by the mapper
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param executorService the {@link ExecutorService} used to execute the computation
	 * @param mapper a function that takes an element and returns a transformed element
	 * @return a stream of transformed elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, ExecutorService executorService, Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.asyncMap(parallelism, asyncTimeoutMillis, executorService, null, () -> mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param mapper the function to apply to each element
	 * @param <R> the type of the result elements
	 * @param executorService the executor service to submit tasks to
	 * @param metricSupplier the metricSupplier will store captured metrics
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.asyncMap(parallelism, this.getAsyncTimeoutMillis(), executorService, metricSupplier, () -> mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param <R> the type of the mapped values
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param executorService the executor service to use for task execution
	 * @param metricSupplier a supplier that provides metrics for tracking the mapping operation's performance
	 * @param mapper the function to apply to each element of the stream
	 * @return an MxStream of mapped values
	 */
	default <R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Function<? super T, ? extends R> mapper) {
		Objects.requireNonNull(mapper);
		return this.asyncMap(parallelism, asyncTimeoutMillis, executorService, metricSupplier, () -> mapper);
	}
	
	/**
	 * Runs the map operation asynchronously using the function provided by the supplier and the default ExecutorService. The asyncMap operation will maintain stream order.
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param supplier the supplier providing the function
	 * @param <R> the type of the result elements
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, Supplier<Function<? super T, ? extends R>> supplier) {
		Objects.requireNonNull(supplier);
		return this.asyncMap(parallelism, null, supplier);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param <R> The type of value produced by the mapping function
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param supplier A supplier of a mapping function that takes an element and returns a new value of type R
	 * @return A new stream that has been mapped asynchronously using the provided function.
	 */
	default <R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, Supplier<Function<? super T, ? extends R>> supplier) {
		return this.asyncMap(parallelism, asyncTimeoutMillis, null, supplier);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param supplier the supplier providing the function
	 * @param <R> the type of the result elements
	 * @param executorService the executor service to submit tasks to
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, ExecutorService executorService, Supplier<Function<? super T, ? extends R>> supplier) {
		return this.asyncMap(parallelism, this.getAsyncTimeoutMillis(), executorService, null, supplier);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param <R> the type of return values of the supplier
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param executorService the executor service to use for the map operation
	 * @param supplier a function that takes an element and returns a new value of type R
	 * @return a stream containing the results of applying the given supplier to each element in this stream
	 */
	default <R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, ExecutorService executorService, Supplier<Function<? super T, ? extends R>> supplier) {
		return this.asyncMap(parallelism, asyncTimeoutMillis, executorService, null, supplier);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param supplier the supplier providing the function
	 * @param <R> the type of the result elements
	 * @param executorService the executor service to submit tasks to
	 * @param metricSupplier the metricSupplier will store captured metrics
	 * @return a new stream with the mapped elements
	 */
	default <R> MxStream<R> asyncMap(int parallelism, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Supplier<Function<? super T, ? extends R>> supplier) {
		return this.asyncMap(parallelism, this.getAsyncTimeoutMillis(), executorService, metricSupplier, supplier);
	}
	
	/**
	 * Runs the map operation asynchronously using the function and ExecutorService provided. The asyncMap operation will maintain stream order.
	 * @param <R> The type of result produced by the mapped function.
	 * @param parallelism the number of tasks to process at one time. This applies back pressure to limit the number of items that are loaded into memory at one time.
	 * @param asyncTimeoutMillis The maximum time (in milliseconds) that the mapping operation may take before it is canceled
	 * @param executorService The executor service used to execute the mapped functions asynchronously.
	 * @param metricSupplier A supplier that produces metrics related to the mapping process.
	 * @param supplier A function that is applied to each result of the asynchronous operations to produce the final result.
	 * @return A MxStream object that emits the mapped results as they become available.
	 */
	<R> MxStream<R> asyncMap(int parallelism, long asyncTimeoutMillis, ExecutorService executorService, AsyncMapMetricSupplier metricSupplier, Supplier<Function<? super T, ? extends R>> supplier);
	
	/**
	 * Batches the elements of the stream into lists of the given size. This operation is the logical opposite of flatMap.
	 * @param batchSize the size of each batch
	 * @return a new stream with the batched elements
	 */
	MxStream<List<T>> batch(int batchSize);
	
	/**
	 * Batches the elements of the stream into lists of the given size or time. Whichever comes first. This operation is the logical opposite of flatMap.
	 * @param batchSize the size of each batch
	 * @return a new stream with the batched elements
	 */
	MxStream<List<T>> batch(int batchSize, long timeout, TimeUnit timeUnit);
	
	/**
	 * Performs an action for each element of the stream.
	 * @param action the action to perform
	 * @return a new stream with the peek operation
	 */
	default MxStream<T> peek(Consumer<? super T> action) {
		Objects.requireNonNull(action);
		return peek(() -> action);
	}
	
	/**
	 * Performs an action for each element of the stream using the action provided by teh supplier.
	 * @param supplier the supplier providing the action
	 * @return a new stream with the peek operation
	 */
	MxStream<T> peek(Supplier<Consumer<? super T>> supplier);
	
	/**
	 * Sorts items in stream using a sliding window to prevent loading all items into memory at one time. Sorting parallel streams will produce approximate results only due to the non-deterministic nature of parallel streams.
	 * @param windowSize The size of the window to consider when sorting elements.
	 * @param comparator A comparator to use for sorting elements. Must be a supertype of {@link Comparator}.
	 * @return A new MxStream instance with sorted elements.
	 */
	default MxStream<T> sorted(int windowSize, Comparator<? super T> comparator) {
		Require.equalOrGreater(windowSize, 1, "windowSize");
		Objects.requireNonNull(comparator);
		return sorted(windowSize, () -> comparator);
	}
	
	/**
	 * Sorts items in stream using a sliding window to prevent loading all items into memory at one time. Sorting parallel streams will produce approximate results only due to the non-deterministic nature of parallel streams.
	 * @param windowSize The size of the sliding window used for sorting. Must be greater than 0.
	 * @param supplier A supplier function that returns a comparator instance, which is used to define the sort order.
	 * @return A new MxStream instance containing elements in sorted order.
	 */
	MxStream<T> sorted(int windowSize, Supplier<Comparator<? super T>> supplier);
	
	/**
	 * Convert this stream into a Producer/Consumer pattern with a single threaded producer that produces events into an ArrayBlockingQueue. One or more consumer threads will read from the ArrayBlockingQueue and process the remaining steps of this stream. If
	 * the stream is already parallel, this does nothing. Parallel streams will process items in a non-deterministic order.
	 * @param parallelism the number of consumer threads that will process items from the buffer.
	 * @param bufferSize size of the ArrayBlockingQueue between the producer and consumer(s).
	 */
	default MxStream<T> fanOut(int parallelism, int bufferSize) {
		Require.equalOrGreater(bufferSize, 1, "bufferSize");
		return this.fanOut(parallelism, bufferSize, null);
	}
	
	/**
	 * Convert this stream into a Producer/Consumer pattern with a single threaded producer that produces events into an ArrayBlockingQueue. One or more consumer threads will read from the ArrayBlockingQueue and process the remaining steps of this stream. If
	 * the stream is already parallel, this does nothing. Parallel streams will process items in a non-deterministic order.
	 * @param parallelism the number of consumer threads that will process items from the buffer.
	 * @param bufferSize size of the ArrayBlockingQueue between the producer and consumer(s).
	 * @param executorService executorService that parallel tasks are submitted to
	 */
	MxStream<T> fanOut(int parallelism, int bufferSize, ExecutorService executorService);
	
	MxStream<T> tap(TapBuilder<T> tapBuilder);
	
	/**
	 * Performs the given action for each element of the stream. This method is a terminal operation and will process all elements.
	 * @param action the action to be performed for each element
	 * @throws StreamException if an error occurs during stream processing
	 * @throws StreamSourceException if the stream source fails during processing
	 * @throws StreamInterruptedException if a stream thread is interrupted during stream processing
	 */
	default void forEach(Consumer<? super T> action) {
		Objects.requireNonNull(action);
		this.forEach(() -> action);
	}
	
	/**
	 * Performs the given action for each element of the stream. This method is a terminal operation and will process all elements.
	 * @param supplier the action supplier to be performed for each element
	 * @throws StreamException if an error occurs during stream processing
	 * @throws StreamSourceException if the stream source fails during processing
	 * @throws StreamInterruptedException if a stream thread is interrupted during stream processing
	 */
	void forEach(Supplier<Consumer<? super T>> supplier);
	
	<R, A> R collect(Collector<? super T, A, R> collector);
	
	/**
	 * Terminate stream with a list of all elements in this stream.
	 * @return a new stream containing the elements from this stream, wrapped in an ArrayList.
	 */
	List<T> toList();
	
	/**
	 * Terminate stream with a set of all elements in this stream.
	 * @return a new stream containing the elements from this stream, wrapped in a HashSet.
	 */
	Set<T> toSet();
	
	/**
	 * Terminate stream with the number of elements in this stream.
	 * @return the number of elements in this stream.
	 */
	long count();
	
	/**
	 * Terminate stream a new standard Java stream containing the elements from this stream.
	 * @return a new stream containing the elements from this stream.
	 */
	Stream<T> toStream();
	
	/**
	 * Creates a new instance of the MxStream class from the given input {@link java.util.stream.Stream}.
	 * @param <T> The type of elements in the Stream.
	 * @param stream The Stream object to create the MxStream from.
	 * @return A new MxStream instance, wrapping the original Stream object.
	 */
	static <T> MxStream<T> of(Stream<T> stream) {
		return new PipelineSource.StreamSource<>(stream);
	}
	
	/**
	 * Creates a new instance of the MxStream class from the given input {@link java.util.stream.Stream}.
	 * @param <T> The type of elements in the input stream.
	 * @param stream The input stream to read data from. This stream is wrapped by the MxStream instance and can be closed when no longer needed.
	 * @param parallelism The degree of parallelism to use for processing elements from the stream. Higher values result in more simultaneous processing, but may consume more resources .
	 * @param executorService The ExecutorService to use for executing tasks. This service is responsible for managing threads that process elements from the stream.
	 * @return A new instance of the MxStream class wrapping the given input stream and configured with the specified parallelism and executor service.
	 */
	static <T> MxStream<T> of(Stream<T> stream, int parallelism, ExecutorService executorService) {
		return new PipelineSource.StreamSource<>(stream, parallelism, executorService);
	}
	
	/**
	 * Creates a new pipeline source from the given iterable.
	 * @param <T> the type of elements in the stream
	 * @param iterator an iterable to create a pipeline source from
	 * @return a new MxStream containing the elements from the stream
	 */
	static <T> MxStream<T> of(Iterator<T> iterator) {
		return new PipelineSource.IteratorSource<>(iterator);
	}
	
	/**
	 * Creates an MxStream from a {@link java.io.BufferedReader}. The BufferedReader will be read line by line and each line will be passed into the stream.
	 * @param bufferedReader The buffered reader to read from.
	 * @return A pipeline source that reads from the specified buffered reader.
	 */
	static MxStream<String> of(BufferedReader bufferedReader) {
		return new PipelineSource.BufferedReaderStream(bufferedReader);
	}
	
	/**
	 * Creates a new pipeline source from the given BufferedReader.
	 * @param <T> the type of elements in the stream
	 * @return a new MxStream containing the elements from the stream
	 */
	static <T> MxStream<T> of(Iterable<T> iterable) {
		return new PipelineSource.IteratorSource<>(iterable.spliterator(), false);
	}
	
}
