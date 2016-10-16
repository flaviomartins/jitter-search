package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.utils.Qrels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QrelsReranker implements Reranker {

    private final Qrels qrels;
    private final String qid;

    public QrelsReranker(List<Document> results, Qrels qrels, String qid) {
        this.qrels = qrels;
        this.qid = qid;
    }

    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        Iterator<Document> resultIt = docs.iterator();

        List<Document> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            Document updatedResult = new Document(origResult);

            int r = qrels.getRel(qid, Long.toString(origResult.getId()));
            updatedResult.setRsv(origResult.getRsv() + r * 100);

            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }

}
