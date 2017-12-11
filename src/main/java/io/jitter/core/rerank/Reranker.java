package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;

import java.io.IOException;
import java.util.List;


public interface Reranker {

    List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) throws IOException;

}
