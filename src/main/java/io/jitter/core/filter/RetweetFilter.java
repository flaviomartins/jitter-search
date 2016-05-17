package io.jitter.core.filter;

import io.jitter.api.search.Document;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RetweetFilter extends Filter {

    public RetweetFilter(List<Document> results) {
        this.results = results;
        this.filter();
    }

    protected void filter() {
        Iterator<Document> resultIt = results.iterator();

        List<Document> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            if (origResult.getRetweeted_status_id() != 0)
                continue;

            if (StringUtils.startsWithIgnoreCase(origResult.getText(), "RT "))
                continue;

            updatedResults.add(origResult);
        }
        results = updatedResults;
    }

}
