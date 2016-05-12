package io.jitter.core.feedback;

import io.jitter.core.utils.TweetUtils;
import org.apache.lucene.analysis.Analyzer;

public class TweetFeedbackRelevanceModel extends FeedbackRelevanceModel {

    public TweetFeedbackRelevanceModel(Analyzer analyzer) {
        super(analyzer);
        setMinTermFreq(1);
    }

    @Override
    public String cleanText(String text) {
        return TweetUtils.clean(text);
    }
}
