package io.jitter.core.rerank;

import io.jitter.api.search.Document;

import java.util.List;

public class IdentityReranker implements Reranker {
    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        return docs;
    }
}
