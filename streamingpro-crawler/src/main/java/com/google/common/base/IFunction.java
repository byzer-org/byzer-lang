package com.google.common.base;

/**
 * Created by allwefantasy on 8/4/2018.
 */
public interface IFunction<F, T> extends java.util.function.Function<F, T>, com.google.common.base.Function<F, T> {
    @Override
    T apply(F input);

    @Override
    boolean equals(Object object);
}
