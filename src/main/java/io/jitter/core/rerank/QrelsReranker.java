package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import io.jitter.core.utils.Qrels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QrelsReranker extends Reranker {

    private final Qrels qrels;
    private final String qid;

    public QrelsReranker(List<Document> results, Qrels qrels, String qid) {
        this.results = results;
        this.qrels = qrels;
        this.qid = qid;
        this.score();
    }

    @Override
    protected void score() {
        Iterator<Document> resultIt = results.iterator();

        List<Document> updatedResults = new ArrayList<>(results.size());
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            Document updatedResult = new Document(origResult);

            int r = qrels.getRel(qid, Long.toString(origResult.getId()));
            updatedResult.setRsv(origResult.getRsv() + r * 100);

            updatedResults.add(updatedResult);
        }
        results = updatedResults;
    }

}
