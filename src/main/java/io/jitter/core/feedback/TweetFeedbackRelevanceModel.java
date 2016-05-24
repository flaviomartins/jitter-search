package io.jitter.core.feedback;

import com.google.common.base.CharMatcher;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.utils.Stopper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

public class TweetFeedbackRelevanceModel extends FeedbackRelevanceModel {

    public TweetFeedbackRelevanceModel(Stopper stopper) {
        super();
        Analyzer analyzer;
        if (stopper == null || stopper.asSet().size() == 0) {
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, true, false, false);
        } else {
            setStopWords(stopper.asSet());
            CharArraySet charArraySet = new CharArraySet(Version.LUCENE_43, stopper.asSet(), true);
            analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, charArraySet, true, false, false);
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
        // no mentions
        if (term.startsWith("@")) {
            return true;
        }
        // no hashtags
        if (term.startsWith("#")) {
            return false;
        }
        // blocks URLs (Regex.VALID_URL is not needed)
        if (!CharMatcher.javaLetterOrDigit().matchesAllOf(term)) {
            return true;
        }
        // allow only ascii chars
        return (!CharMatcher.ascii().matchesAllOf(term));
    }

}
