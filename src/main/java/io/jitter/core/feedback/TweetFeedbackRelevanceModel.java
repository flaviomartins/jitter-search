package io.jitter.core.feedback;

import org.apache.lucene.analysis.Analyzer;

public class TweetFeedbackRelevanceModel extends FeedbackRelevanceModel {

    public TweetFeedbackRelevanceModel(Analyzer analyzer) {
        super(analyzer);
        setMinWordLen(1);
        setMinDocFreq(10);
    }

    @Override
    public boolean isNoiseWord(String term) {
        if (super.isNoiseWord(term)) {
            return true;
        }
        return (term.startsWith("@"));
    }

}
