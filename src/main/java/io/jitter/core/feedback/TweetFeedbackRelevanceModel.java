package io.jitter.core.feedback;

import com.google.common.base.CharMatcher;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;

public class TweetFeedbackRelevanceModel extends FeedbackRelevanceModel {

    public TweetFeedbackRelevanceModel(Stopper stopper) {
        super();
        Analyzer analyzer;
        if (stopper == null || stopper.asSet().isEmpty()) {
            analyzer = new TweetAnalyzer(CharArraySet.EMPTY_SET);
        } else {
            CharArraySet charArraySet = new CharArraySet(stopper.asSet(), true);
            analyzer = new TweetAnalyzer(charArraySet);
        }
        setAnalyzer(analyzer);
        setMinWordLen(3);
        setMinDocFreq(10);
        setMinTermFreq(1);
    }

    @Override
    public boolean isNoiseWord(String term) {
        if (super.isNoiseWord(term)) {
            return true;
        }
        // allow hashtags
        if (term.startsWith("#")) {
            return false;
        }
        // no mentions
        if (term.startsWith("@")) {
            return true;
        }
        // no rt
        if ("rt".equals(term)) {
            return true;
        }
        // no URLs (Regex.VALID_URL is not needed)
        if (term.contains("/")) {
            return true;
        }
        // allow only ascii chars
        return (!CharMatcher.ascii().matchesAllOf(term));
    }

}
