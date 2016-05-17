package io.jitter.core.feedback;

import com.google.common.base.CharMatcher;
import org.apache.lucene.analysis.Analyzer;

public class TweetFeedbackRelevanceModel extends FeedbackRelevanceModel {

    public TweetFeedbackRelevanceModel(Analyzer analyzer) {
        super(analyzer);
        setMinWordLen(3);
        setMinDocFreq(10);
        setMinTermFreq(1);
    }

    @Override
    public boolean isNoiseWord(String term) {
        if (super.isNoiseWord(term)) {
            return true;
        }
        if (term.startsWith("@")) {
            return true;
        }
        return (!CharMatcher.ascii().matchesAllOf(term));
    }

}
