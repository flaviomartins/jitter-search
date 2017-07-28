package io.jitter.core.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;

/** An {@link Analyzer} that filters {@link KeywordTokenizer}
 *  with {@link LowerCaseFilter}
 **/
public final class LowercaseKeywordAnalyzer extends Analyzer {

    /**
     * Creates a new {@link LowercaseKeywordAnalyzer}
     */
    public LowercaseKeywordAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer source = new KeywordTokenizer();
        return new TokenStreamComponents(source, new LowerCaseFilter(source));
    }
}
