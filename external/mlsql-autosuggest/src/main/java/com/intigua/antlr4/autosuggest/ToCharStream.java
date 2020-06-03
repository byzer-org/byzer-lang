package com.intigua.antlr4.autosuggest;

import org.antlr.v4.runtime.CharStream;

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public interface ToCharStream {
    public CharStream toCharStream(String text);
}
