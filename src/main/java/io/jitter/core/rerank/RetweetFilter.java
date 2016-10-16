package io.jitter.core.rerank;

import io.jitter.api.search.Document;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RetweetFilter implements Reranker {

    public RetweetFilter() {
    }

    @Override
    public List<Document> rerank(List<Document> docs, RerankerContext context) {
        Iterator<Document> resultIt = docs.iterator();

        List<Document> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            if (origResult.getRetweeted_status_id() != 0)
                continue;

            if (StringUtils.startsWithIgnoreCase(origResult.getText(), "RT "))
                continue;

            updatedResults.add(origResult);
        }
        return updatedResults;
    }

}
