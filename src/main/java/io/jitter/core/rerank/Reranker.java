package io.jitter.core.rerank;

import io.jitter.api.search.Document;

import java.util.List;


public interface Reranker {

    List<Document> rerank(List<Document> docs, RerankerContext context);

}
