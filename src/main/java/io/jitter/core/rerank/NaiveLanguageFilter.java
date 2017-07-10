package io.jitter.core.rerank;

import com.google.common.base.CharMatcher;
import io.jitter.api.search.StatusDocument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NaiveLanguageFilter implements Reranker {
    private final String lang;
    
    public NaiveLanguageFilter(String lang) {
        this.lang = lang;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> docs, RerankerContext context) {
        Iterator<StatusDocument> resultIt = docs.iterator();

        List<StatusDocument> updatedResults = new ArrayList<>();
        while (resultIt.hasNext()) {
            StatusDocument origResult = resultIt.next();

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
        return updatedResults;
    }

    private static boolean isProbablyEnglish(String text) {
        // Replace invisible characters
        String normalized = CharMatcher.invisible().replaceFrom(text, " ");
        // Replace unicode whitespace
        normalized = CharMatcher.whitespace().replaceFrom(normalized, " ");
        return CharMatcher.ascii().matchesAllOf(normalized);
    }

}
