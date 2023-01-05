package com.aliyun.dts.subscribe.clients.common;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Copy code from java.util.Optional, the only difference is NullableOptional consider null as a normal value.
 * So the NullableOptional.ofNullable(null).isPresent() should be true.
 */
public final class NullableOptional<T> {
    private static final NullableOptional<?> EMPTY = new NullableOptional();
    private final T value;
    private final boolean isPresent;

    private NullableOptional() {
        this.isPresent = false;
        this.value = null;
    }

    public static <T> NullableOptional<T> empty() {
        NullableOptional value = EMPTY;
        return value;
    }

    private NullableOptional(T value) {
        this.value = value;
        this.isPresent = true;
    }

    public static <T> NullableOptional<T> of(T value) {
        Objects.requireNonNull(value);
        return new NullableOptional(value);
    }

    public static <T> NullableOptional<T> ofNullable(T value) {
        return new NullableOptional<>(value);
    }

    public T get() {
        if (!this.isPresent()) {
            throw new NoSuchElementException("No value present");
        } else {
            return this.value;
        }
    }

    public boolean isPresent() {
        return this.isPresent;
    }

    public void ifPresent(Consumer<? super T> consumer) {
        if (this.isPresent()) {
            consumer.accept(this.value);
        }
    }

    public NullableOptional<T> filter(Predicate<? super T> filterFunction) {
        Objects.requireNonNull(filterFunction);
        if (!this.isPresent()) {
            return this;
        } else {
            return filterFunction.test(this.value) ? this : empty();
        }
    }

    public <U> NullableOptional<U> map(Function<? super T, ? extends U> function) {
        Objects.requireNonNull(function);
        return !this.isPresent() ? empty() : ofNullable(function.apply(this.value));
    }

    public <U> NullableOptional<U> flatMap(Function<? super T, NullableOptional<U>> mapper) {
        Objects.requireNonNull(mapper);
        return !this.isPresent() ? empty() : (NullableOptional) Objects.requireNonNull(mapper.apply(this.value));
    }

    public T orElse(T defaultValue) {
        return this.isPresent() ? this.value : defaultValue;
    }

    public T orElseGet(Supplier<? extends T> defaultValueSupplier) {
        return this.isPresent() ? this.value : defaultValueSupplier.get();
    }

    public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
        if (this.isPresent()) {
            return this.value;
        } else {
            throw exceptionSupplier.get();
        }
    }

    public boolean equals(Object var1) {
        if (this == var1) {
            return true;
        } else if (!(var1 instanceof NullableOptional)) {
            return false;
        } else {
            NullableOptional var2 = (NullableOptional) var1;
            return Objects.equals(this.isPresent, var2.isPresent) && Objects.equals(this.value, var2.value);
        }
    }

    public int hashCode() {
        return Objects.hash(this.isPresent, this.value);
    }

    public String toString() {
        return this.isPresent() ? String.format("Optional[%s]", this.value) : "Optional.empty";
    }
}
