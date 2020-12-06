package io.jitter.core.rerank;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;

public class RerankerContext {
  private final IndexSearcher searcher;
  private final Query query;
  private final String queryId;
  private final String queryText;
  private final double queryEpoch;
  private final List<String> queryTokens;
  private final Query filter;
  private final String termVectorField;

  public RerankerContext(IndexSearcher searcher, Query query, String queryId, String queryText,
                         double queryEpoch, List<String> queryTokens, String termVectorField, Query filter) throws IOException {
    this.searcher = searcher;
    this.query = query;
    this.queryId = queryId;
    this.queryText = queryText;
    this.queryEpoch = queryEpoch;
    this.queryTokens = queryTokens;
    this.filter = filter;
    this.termVectorField = termVectorField;
  }

  public IndexSearcher getIndexSearcher() {
    return searcher;
  }

  public Query getFilter() {
    return filter;
  }

  public Query getQuery() {
    return query;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getQueryText() {
    return queryText;
  }

  public double getQueryEpoch() {
    return queryEpoch;
  }

  public List<String> getQueryTokens() {
    return queryTokens;
  }

  public String getField() {return termVectorField; }
}
