/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jitter.core.analysis;

import java.io.Reader;

import com.google.common.base.CharMatcher;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;


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

    public StopperTweetAnalyzer(Version matchVersion, boolean stemming, boolean preserveCaps, boolean possessiveFiltering) {
        this(matchVersion, STOP_WORDS_SET, stemming, preserveCaps, possessiveFiltering);
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
        Tokenizer source = new CharTokenizer(matchVersion, reader) {
            @Override
            protected boolean isTokenChar(int c) {
                return !CharMatcher.WHITESPACE.matches((char)c);
            }
        };
        TokenStream filter;
        if (possessiveFiltering) {
            filter = new EnglishPossessiveFilter(matchVersion, source);
            filter = new EntityPreservingFilter(filter, preserveCaps);
        } else {
            filter = new EntityPreservingFilter(source, preserveCaps);
        }

        filter = new StopFilter(filter, stopwords);

        if (stemming) {
            // Porter stemmer ignores words which are marked as keywords
            filter = new PorterStemFilter(filter);
        }
        return new TokenStreamComponents(source, filter);
    }

}