package io.jitter.core.rerank;

import com.google.common.collect.Lists;
import io.jitter.api.search.StatusDocument;

import java.io.IOException;
import java.util.List;

/**
 * Representation of a cascade of rerankers, applied in sequence.
 */
public class RerankerCascade {
  final List<Reranker> rerankers = Lists.newArrayList();

  /**
   * Adds a reranker to this cascade.
   *
   * @param reranker reranker to add
   * @return this cascade for method chaining
   */
  public RerankerCascade add(Reranker reranker) {
    rerankers.add(reranker);

    return this;
  }

  /**
   * Runs this cascade.
   *
   * @param docs input documents
   * @return reranked results
   */
  public List<StatusDocument> run(List<StatusDocument> docs, RerankerContext context) throws IOException {
    List<StatusDocument> results = docs;

    for (Reranker reranker : rerankers) {
      results = reranker.rerank(results, context);
    }

    return results;
  }
}
