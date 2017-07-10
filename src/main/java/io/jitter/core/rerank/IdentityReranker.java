package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;

import java.util.List;

public class IdentityReranker implements Reranker {
    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        return docs;
    }
}
