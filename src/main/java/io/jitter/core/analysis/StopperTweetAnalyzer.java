package io.jitter.core.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import java.io.Reader;

public final class StopperTweetAnalyzer extends StopwordAnalyzerBase {

    /**
     * An unmodifiable set containing some common English words that are usually not
     * useful for searching.
     */
    public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

    private final Version matchVersion;
    private final boolean stemming;
    private final boolean preserveCaps;
    private final boolean possessiveFiltering;

    public StopperTweetAnalyzer(Version matchVersion, CharArraySet stopWords, boolean stemming, boolean preserveCaps, boolean possessiveFiltering) {
        super(stopWords);
        this.matchVersion = matchVersion;
        this.stemming = stemming;
        this.preserveCaps = preserveCaps;
        this.possessiveFiltering = possessiveFiltering;
    }

    public StopperTweetAnalyzer(Version matchVersion, CharArraySet stopWords, boolean stemming, boolean preserveCaps) {
        super(stopWords);
        this.matchVersion = matchVersion;
        this.stemming = stemming;
        this.preserveCaps = preserveCaps;
        this.possessiveFiltering = false;
    }

    public StopperTweetAnalyzer(Version matchVersion, CharArraySet stopWords, boolean stemming) {
        this(matchVersion, stopWords, stemming, false);
    }

    public StopperTweetAnalyzer(Version matchVersion, boolean stemming, boolean preserveCaps) {
        this(matchVersion, STOP_WORDS_SET, stemming, preserveCaps);
    }
    
    public StopperTweetAnalyzer(Version matchVersion, boolean stemming) {
        this(matchVersion, STOP_WORDS_SET, stemming);
    }

    public StopperTweetAnalyzer(Version matchVersion) {
        this(matchVersion, true);
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
        Tokenizer source = new WhitespaceTokenizer(reader);
        TokenStream tok;
        if (possessiveFiltering) {
            tok = new EnglishPossessiveFilter(matchVersion, source);
            tok = new EntityPreservingFilter(tok, preserveCaps);
        } else {
            tok = new EntityPreservingFilter(source, preserveCaps);
        }

        tok = new StopFilter(tok, stopwords);

        if (stemming) {
            // Porter stemmer ignores words which are marked as keywords
            tok = new PorterStemFilter(tok);
        }
        return new TokenStreamComponents(source, tok);
    }

}
