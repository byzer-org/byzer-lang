package streaming.dsl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.misc.Interval;

/**
 * Created by fchen on 2018/9/6.
 */
public class CaseChangingCharStream implements CharStream {

  final CharStream stream;
  final boolean upper;

  /**
   * Constructs a new CaseChangingCharStream wrapping the given {@link CharStream} forcing
   * all characters to upper case or lower case.
   * @param stream The stream to wrap.
   * @param upper If true force each symbol to upper case, otherwise force to lower.
   */
  public CaseChangingCharStream(CharStream stream, boolean upper) {
    this.stream = stream;
    this.upper = upper;
  }

  public CaseChangingCharStream(CharStream stream) {
    this.stream = stream;
    this.upper = true;
  }

  public CaseChangingCharStream(String string) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(string.getBytes());
    this.stream = CharStreams.fromStream(bais);
    this.upper = false;
  }
  @Override
  public String getText(Interval interval) {
    return stream.getText(interval);
  }

  @Override
  public void consume() {
    stream.consume();
  }

  @Override
  public int LA(int i) {
    int c = stream.LA(i);
    if (c <= 0) {
      return c;
    }
    if (upper) {
      return Character.toUpperCase(c);
    }
    return Character.toLowerCase(c);
  }

  @Override
  public int mark() {
    return stream.mark();
  }

  @Override
  public void release(int marker) {
    stream.release(marker);
  }

  @Override
  public int index() {
    return stream.index();
  }

  @Override
  public void seek(int index) {
    stream.seek(index);
  }

  @Override
  public int size() {
    return stream.size();
  }

  @Override
  public String getSourceName() {
    return stream.getSourceName();
  }
}
