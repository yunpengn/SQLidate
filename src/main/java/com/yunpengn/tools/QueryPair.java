package com.yunpengn.tools;

import java.util.Objects;

/**
 * An abstraction to store the query input format.
 */
public class QueryPair {
  public final String origin;
  public final String first;
  public final String second;

  public QueryPair(final String origin, final String first, final String second) {
    this.origin = origin;
    this.first = first;
    this.second = second;
  }

  @Override public boolean equals(final Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    } else {
      final QueryPair queryPair = (QueryPair) other;
      return origin.equals(queryPair.origin)
          && first.equals(queryPair.first)
          && second.equals(queryPair.second);
    }
  }

  @Override public int hashCode() {
    return Objects.hash(origin, first, second);
  }
}
