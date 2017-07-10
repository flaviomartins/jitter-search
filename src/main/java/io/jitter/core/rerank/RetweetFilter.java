package io.jitter.core.rerank;

import io.jitter.api.search.StatusDocument;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RetweetFilter implements Reranker {

    public RetweetFilter() {
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        Iterator<StatusDocument> resultIt = docs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();

            if (origResult.getRetweeted_status_id() != 0)
                continue;

            if (StringUtils.startsWithIgnoreCase(origResult.getText(), "RT "))
                continue;

            updatedResults.add(origResult);
        }
        return updatedResults;
    }

}
