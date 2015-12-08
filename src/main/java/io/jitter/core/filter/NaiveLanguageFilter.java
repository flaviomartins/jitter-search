package io.jitter.core.filter;

import com.google.common.base.CharMatcher;
import io.jitter.api.search.Document;
import io.jitter.core.utils.TweetUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NaiveLanguageFilter extends SearchFilter {
    private final String lang;

    @SuppressWarnings("SameParameterValue")
    public NaiveLanguageFilter(String lang) {
        this.lang = lang;
    }

    public void setResults(List<Document> results) {
        this.results = results;
        this.filter();
    }

    protected void filter() {
        Iterator<Document> resultIt = results.iterator();

        List<Document> updatedResults = new ArrayList<Document>();
        while (resultIt.hasNext()) {
            Document origResult = resultIt.next();

            // Hit has language annotation
            if (origResult.getLang() != null) {
                // Hit is annotated as different language
                if (!lang.equals(origResult.getLang())) {
                    continue;
                }
            } else {
                if (lang.equals("en")) {
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
        text = TweetUtils.clean(text);
        text = text.replaceAll("[\\p{Z}\\p{S}\\p{P}\\p{C}]+", " ");
        // Replace invisible characters
        text = CharMatcher.INVISIBLE.replaceFrom(text, " ");
        // Replace unicode whitespace
        text = CharMatcher.WHITESPACE.replaceFrom(text, " ");
        // Remove other stuff
        boolean isASCII = CharMatcher.ASCII.matchesAllOf(text);
        return isASCII;
    }

}
