package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.utils.Qrels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QrelsReranker implements Reranker {

    private final Qrels qrels;
    private final String qid;

    public QrelsReranker(List<StatusDocument> results, Qrels qrels, String qid) {
        this.qrels = qrels;
        this.qid = qid;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        Iterator<StatusDocument> resultIt = docs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>(docs.size());
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();

            StatusDocument updatedResult = new StatusDocument(origResult);

            int r = qrels.getRel(qid, origResult.getId());
            updatedResult.setRsv(origResult.getRsv() + r * 100);

            updatedResults.add(updatedResult);
        }
        return updatedResults;
    }

}
