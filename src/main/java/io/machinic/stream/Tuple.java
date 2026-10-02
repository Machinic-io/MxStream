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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Tuple {
	
	public static <T> Unit<T> of(T value) {
		return new Unit<>(value);
	}
	
	public static <L, R> Pair<L, R> of(L left, R right) {
		return new Pair<>(left, right);
	}
	
	public static <L, C, R> Triplet<L, C, R> of(L left, C center, R right) {
		return new Triplet<>(left, center, right);
	}
	
	public static <A, B, C, D> Quad<A, B, C, D> of(A first, B second, C third, D fourth) {
		return new Quad<>(first, second, third, fourth);
	}
	
	public static class Unit<T> {
		private final T value;
		
		@JsonCreator
		public Unit(@JsonAlias({ "value", "left" }) @JsonProperty("first") T value) {
			this.value = value;
		}
		
		@JsonIgnore
		public T getLeft() {
			return value;
		}
		
		public T getFirst() {
			return value;
		}
		
		public <R> Pair<T, R> append(R right) {
			return new Pair<>(value, right);
		}
	}
	
	public static class Pair<L, R> {
		private final L left;
		private final R right;
		
		@JsonCreator
		public Pair(
				@JsonAlias({ "left" }) @JsonProperty("first") L left,
				@JsonAlias({ "right" }) @JsonProperty("second") R right) {
			this.left = left;
			this.right = right;
		}
		
		@JsonIgnore
		public L getLeft() {
			return left;
		}
		
		public L getFirst() {
			return left;
		}
		
		@JsonIgnore
		public R getRight() {
			return right;
		}
		
		public R getSecond() {
			return right;
		}
		
		public <T> Triplet<L, R, T> append(T value) {
			return new Triplet<>(left, right, value);
		}
		
	}
	
	public static class Triplet<L, C, R> {
		private final L left;
		private final C center;
		private final R right;
		
		@JsonCreator
		public Triplet(
				@JsonAlias({ "left" }) @JsonProperty("first") L left,
				@JsonAlias({ "center" }) @JsonProperty("second") C center,
				@JsonAlias({ "right" }) @JsonProperty("third") R right) {
			this.left = left;
			this.center = center;
			this.right = right;
		}
		
		@JsonIgnore
		public L getLeft() {
			return left;
		}
		
		public L getFirst() {
			return left;
		}
		
		@JsonIgnore
		public C getCenter() {
			return center;
		}
		
		public C getSecond() {
			return center;
		}
		
		public R getRight() {
			return right;
		}
		
		@JsonIgnore
		public R getThird() {
			return right;
		}
		
		public <T> Quad<L, C, R, T> append(T value) {
			return new Quad<>(left, center, right, value);
		}
		
	}
	
	public static class Quad<A, B, C, D> {
		private final A first;
		private final B second;
		private final C third;
		private final D fourth;
		
		@JsonCreator
		public Quad(
				@JsonProperty("first") A first,
				@JsonProperty("second") B second,
				@JsonProperty("third") C third,
				@JsonProperty("fourth") D fourth) {
			this.first = first;
			this.second = second;
			this.third = third;
			this.fourth = fourth;
		}
		
		@JsonIgnore
		public A getLeft() {
			return first;
		}
		
		public A getFirst() {
			return first;
		}
		
		public B getSecond() {
			return second;
		}
		
		public C getThird() {
			return third;
		}
		
		public D getFourth() {
			return fourth;
		}
		
		@JsonIgnore
		public D getRight() {
			return fourth;
		}
	}
	
}
