package io.jitter.core.filter;

import com.google.common.base.CharMatcher;
import io.jitter.api.search.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NaiveLanguageFilter extends Filter {
    private final String lang;
    
    public NaiveLanguageFilter(String lang) {
        this.lang = lang;
    }

    public void setResults(List<Document> results) {
        this.results = results;
        this.filter();
    }

    protected void filter() {
        Iterator<Document> resultIt = results.iterator();

        List<Document> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            // Hit has language annotation
            if (origResult.getLang() != null) {
                // Hit is annotated as different language
                if (!lang.equals(origResult.getLang())) {
                    continue;
                }
            } else {
                if ("en".equals(lang)) {
                    // For English: Skip only if text contains non ASCII chars
                    if (!isProbablyEnglish(origResult.getText()))
                        continue;
                }
            }

            updatedResults.add(origResult);
        }
        results = updatedResults;
    }

    private boolean isProbablyEnglish(String text) {
        // Replace invisible characters
        text = CharMatcher.invisible().replaceFrom(text, " ");
        // Replace unicode whitespace
        text = CharMatcher.whitespace().replaceFrom(text, " ");
        return CharMatcher.ascii().matchesAllOf(text);
    }

}
