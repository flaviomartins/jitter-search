package io.jitter.core.feedback;

import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.document.DocVector;
import io.jitter.core.document.FeatureVector;
import io.jitter.core.utils.AnalyzerUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;

import java.io.IOException;
import java.util.*;


public class FeedbackRelevanceModel {

    /**
     * Ignore terms with less than this frequency in the source doc.
     *
     * @see #getMinTermFreq
     * @see #setMinTermFreq
     */
    public static final int DEFAULT_MIN_TERM_FREQ = 2;

    /**
     * Ignore words which do not occur in at least this many docs.
     *
     * @see #getMinDocFreq
     * @see #setMinDocFreq
     */
    public static final int DEFAULT_MIN_DOC_FREQ = 5;

    /**
     * Ignore words which occur in more than this many docs.
     *
     * @see #getMaxDocFreq
     * @see #setMaxDocFreq
     * @see #setMaxDocFreqPct
     */
    public static final int DEFAULT_MAX_DOC_FREQ = Integer.MAX_VALUE;

    /**
     * Boost terms in query based on score.
     *
     * @see #isBoost
     * @see #setBoost
     */
    public static final boolean DEFAULT_BOOST = false;

    /**
     * Default field names. Null is used to specify that the field names should be looked
     * up at runtime from the provided reader.
     */
    public static final String[] DEFAULT_FIELD_NAMES = new String[]{"contents"};

    /**
     * Ignore words less than this length or if 0 then this has no effect.
     *
     * @see #getMinWordLen
     * @see #setMinWordLen
     */
    public static final int DEFAULT_MIN_WORD_LENGTH = 0;

    /**
     * Ignore words greater than this length or if 0 then this has no effect.
     *
     * @see #getMaxWordLen
     * @see #setMaxWordLen
     */
    public static final int DEFAULT_MAX_WORD_LENGTH = 0;

    /**
     * Default set of stopwords.
     * If null means to allow stop words.
     *
     * @see #setStopWords
     * @see #getStopWords
     */
    public static final Set<?> DEFAULT_STOP_WORDS = null;

    /**
     * Current set of stop words.
     */
    private Set<?> stopWords = DEFAULT_STOP_WORDS;

    /**
     * Return a Query with no more than this many terms.
     *
     * @see BooleanQuery#getMaxClauseCount
     * @see #getMaxQueryTerms
     * @see #setMaxQueryTerms
     */
    public static final int DEFAULT_MAX_QUERY_TERMS = 25;

    /**
     * Analyzer that will be used to parse the doc.
     */
    private Analyzer analyzer = null;

    /**
     * Ignore words less frequent that this.
     */
    private int minTermFreq = DEFAULT_MIN_TERM_FREQ;

    /**
     * Ignore words which do not occur in at least this many docs.
     */
    private int minDocFreq = DEFAULT_MIN_DOC_FREQ;

    /**
     * Ignore words which occur in more than this many docs.
     */
    private int maxDocFreq = DEFAULT_MAX_DOC_FREQ;

    /**
     * Should we apply a boost to the Query based on the scores?
     */
    private boolean boost = DEFAULT_BOOST;

    /**
     * Field name we'll analyze.
     */
    private String[] fieldNames = DEFAULT_FIELD_NAMES;

    /**
     * Ignore words if less than this len.
     */
    private int minWordLen = DEFAULT_MIN_WORD_LENGTH;

    /**
     * Ignore words if greater than this len.
     */
    private int maxWordLen = DEFAULT_MAX_WORD_LENGTH;

    /**
     * Don't return a query longer than this.
     */
    private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

    /**
     * For idf() calculations.
     */
    private TFIDFSimilarity similarity;// = new DefaultSimilarity();

    /**
     * For numTerms() and docFreq() calculations.
     */
    private CollectionStats collectionStats;

    /**
     * Boost factor to use when boosting the terms
     */
    private float boostFactor = 1;

    /**
     * Returns the boost factor used when boosting terms
     *
     * @return the boost factor used when boosting terms
     * @see #setBoostFactor(float)
     */
    public float getBoostFactor() {
        return boostFactor;
    }

    /**
     * Sets the boost factor to use when boosting terms
     *
     * @see #getBoostFactor()
     */
    public void setBoostFactor(float boostFactor) {
        this.boostFactor = boostFactor;
    }

    private double[] docWeights = null;

    /**
     * Returns the docWeights to use when boosting docs
     *
     * @return the docWeights to use when boosting docs
     * @see #setDocWeights(double[])
     */
    public double[] getDocWeights() {
        return docWeights;
    }

    /**
     * Sets the docWeights to use when boosting docs
     *
     * @see #getDocWeights()
     */
    public void setDocWeights(double[] docWeights) {
        this.docWeights = docWeights;
    }

    public FeedbackRelevanceModel() {
        this(new StandardAnalyzer(), new DefaultSimilarity());
    }

    public FeedbackRelevanceModel(Analyzer analyzer) {
        this(analyzer, new DefaultSimilarity());
    }

    public FeedbackRelevanceModel(Analyzer analyzer, TFIDFSimilarity sim) {
        this.analyzer = analyzer;
        this.similarity = sim;
    }

    public CollectionStats getCollectionStats() {
        return collectionStats;
    }

    public void setCollectionStats(CollectionStats collectionStats) {
        this.collectionStats = collectionStats;
    }

    /**
     * Returns an analyzer that will be used to parse source doc with. The default analyzer
     * is not set.
     *
     * @return the analyzer that will be used to parse source doc with.
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Sets the analyzer to use. An analyzer is not required for generating a query with the
     * {@link #like(List<Document>)} method, all other 'like' methods require an analyzer.
     *
     * @param analyzer the analyzer to use to tokenize text.
     */
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    /**
     * Returns the frequency below which terms will be ignored in the source doc. The default
     * frequency is the {@link #DEFAULT_MIN_TERM_FREQ}.
     *
     * @return the frequency below which terms will be ignored in the source doc.
     */
    public int getMinTermFreq() {
        return minTermFreq;
    }

    /**
     * Sets the frequency below which terms will be ignored in the source doc.
     *
     * @param minTermFreq the frequency below which terms will be ignored in the source doc.
     */
    public void setMinTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
    }

    /**
     * Returns the frequency at which words will be ignored which do not occur in at least this
     * many docs. The default frequency is {@link #DEFAULT_MIN_DOC_FREQ}.
     *
     * @return the frequency at which words will be ignored which do not occur in at least this
     *         many docs.
     */
    public int getMinDocFreq() {
        return minDocFreq;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs.
     *
     * @param minDocFreq the frequency at which words will be ignored which do not occur in at
     * least this many docs.
     */
    public void setMinDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

    /**
     * Returns the maximum frequency in which words may still appear.
     * Words that appear in more than this many docs will be ignored. The default frequency is
     * {@link #DEFAULT_MAX_DOC_FREQ}.
     *
     * @return get the maximum frequency at which words are still allowed,
     *         words which occur in more docs than this are ignored.
     */
    public int getMaxDocFreq() {
        return maxDocFreq;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored.
     *
     * @param maxFreq the maximum count of documents that a term may appear
     * in to be still considered relevant
     */
    public void setMaxDocFreq(int maxFreq) {
        this.maxDocFreq = maxFreq;
    }

    /**
     * Set the maximum percentage in which words may still appear. Words that appear
     * in more than this many percent of all docs will be ignored.
     *
     * @param maxPercentage the maximum percentage of documents (0-100) that a term may appear
     * in to be still considered relevant
     */
    public void setMaxDocFreqPct(int maxPercentage) {
        this.maxDocFreq = maxPercentage * collectionStats.numDocs() / 100;
    }


    /**
     * Returns whether to boost terms in query based on "score" or not. The default is
     * {@link #DEFAULT_BOOST}.
     *
     * @return whether to boost terms in query based on "score" or not.
     * @see #setBoost
     */
    public boolean isBoost() {
        return boost;
    }

    /**
     * Sets whether to boost terms in query based on "score" or not.
     *
     * @param boost true to boost terms in query based on "score", false otherwise.
     * @see #isBoost
     */
    public void setBoost(boolean boost) {
        this.boost = boost;
    }

    /**
     * Returns the field names that will be used when generating the 'More Like This' query.
     * The default field names that will be used is {@link #DEFAULT_FIELD_NAMES}.
     *
     * @return the field names that will be used when generating the 'More Like This' query.
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * Sets the field names that will be used when generating the 'More Like This' query.
     * Set this to null for the field names to be determined at runtime from the IndexReader
     * provided in the constructor.
     *
     * @param fieldNames the field names that will be used when generating the 'More Like This'
     * query.
     */
    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Returns the minimum word length below which words will be ignored. Set this to 0 for no
     * minimum word length. The default is {@link #DEFAULT_MIN_WORD_LENGTH}.
     *
     * @return the minimum word length below which words will be ignored.
     */
    public int getMinWordLen() {
        return minWordLen;
    }

    /**
     * Sets the minimum word length below which words will be ignored.
     *
     * @param minWordLen the minimum word length below which words will be ignored.
     */
    public void setMinWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
    }

    /**
     * Returns the maximum word length above which words will be ignored. Set this to 0 for no
     * maximum word length. The default is {@link #DEFAULT_MAX_WORD_LENGTH}.
     *
     * @return the maximum word length above which words will be ignored.
     */
    public int getMaxWordLen() {
        return maxWordLen;
    }

    /**
     * Sets the maximum word length above which words will be ignored.
     *
     * @param maxWordLen the maximum word length above which words will be ignored.
     */
    public void setMaxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
    }

    /**
     * Set the set of stopwords.
     * Any word in this set is considered "uninteresting" and ignored.
     * Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
     * for the purposes of document similarity it seems reasonable to assume that "a stop word is never interesting".
     *
     * @param stopWords set of stopwords, if null it means to allow stop words
     * @see #getStopWords
     */
    public void setStopWords(Set<?> stopWords) {
        this.stopWords = stopWords;
    }

    /**
     * Get the current stop words being used.
     *
     * @see #setStopWords
     */
    public Set<?> getStopWords() {
        return stopWords;
    }


    /**
     * Returns the maximum number of query terms that will be included in any generated query.
     * The default is {@link #DEFAULT_MAX_QUERY_TERMS}.
     *
     * @return the maximum number of query terms that will be included in any generated query.
     */
    public int getMaxQueryTerms() {
        return maxQueryTerms;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     *
     * @param maxQueryTerms the maximum number of query terms that will be included in any
     * generated query.
     */
    public void setMaxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }

    /**
     * Describe the parameters that control how the "more like this" query is formed.
     */
    public String describeParams() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("\t").append("maxQueryTerms  : ").append(maxQueryTerms).append("\n");
        sb.append("\t").append("minWordLen     : ").append(minWordLen).append("\n");
        sb.append("\t").append("maxWordLen     : ").append(maxWordLen).append("\n");
        sb.append("\t").append("fieldNames     : ");
        String delim = "";
        for (String fieldName : fieldNames) {
            sb.append(delim).append(fieldName);
            delim = ", ";
        }
        sb.append("\n");
        sb.append("\t").append("boost          : ").append(boost).append("\n");
        sb.append("\t").append("minTermFreq    : ").append(minTermFreq).append("\n");
        sb.append("\t").append("minDocFreq     : ").append(minDocFreq).append("\n");
        return sb.toString();
    }

    /**
     * determines if the passed term is likely to be of interest in "more like" comparisons
     *
     * @param term The word being considered
     * @return true if should be ignored, false if should be used in further analysis
     */
    public boolean isNoiseWord(String term) {
        int len = term.length();
        if (minWordLen > 0 && len < minWordLen) {
            return true;
        }
        if (maxWordLen > 0 && len > maxWordLen) {
            return true;
        }
        return stopWords != null && stopWords.contains(term);
    }

    /**
     * determines if the passed term is likely to be of interest in "more like" comparisons
     *
     * @param term The word being considered
     * @return true if should be ignored, false if should be used in further analysis
     */
    private boolean isUnfreqWord(String term) {
        int docFreq = collectionStats.docFreq(term);
        return minDocFreq > 0 && docFreq < minDocFreq;
    }

    public FeatureVector like(List<Document> relDocs) throws IOException {
        int numDocVectors = relDocs.size();
        FeatureVector f = new FeatureVector();
        Set<String> vocab = new HashSet<>();
        DocVector[] docVectors = new DocVector[numDocVectors];
        double[] scores = new double[numDocVectors];

        for (int i = 0; i < numDocVectors; i++) {
            Document hit = relDocs.get(i);
            double rsv = hit.getRsv();
            if (rsv >= 0) {
                scores[i] = rsv;
            } else {
                scores[i] = Math.exp(rsv);
            }

            DocVector docVector = hit.getDocVector();
            // if the term vectors are unavailable generate it here
            if (docVector == null) {
                DocVector aDocVector = new DocVector();
                List<String> docTerms = AnalyzerUtils.analyze(analyzer, hit.getText());

                for (String t : docTerms) {
                    if (!t.isEmpty()) {
                        Integer n = aDocVector.vector.get(t);
                        n = (n == null) ? 1 : ++n;
                        aDocVector.vector.put(t, n);
                    }
                }

                docVector = aDocVector;
            }

            vocab.addAll(docVector.vector.keySet());
            docVectors[i] = docVector;
        }

        // Precompute the norms once and cache results.
        float[] norms = new float[numDocVectors];
        for (int i = 0; i < docVectors.length; i++) {
            norms[i] = (float) docVectors[i].computeL1Norm();
        }

        for (String term : vocab) {
            // TODO: minTermFreq filtering
            if (isNoiseWord(term) || isUnfreqWord(term))
                continue;

            float fbWeight = 0.0f;
            for (int i = 0; i < docVectors.length; i++) {
                int termFreq = docVectors[i].getTermFreq(term);
                if (termFreq > 0) {
                    fbWeight += (termFreq / norms[i]) * scores[i];
                }
            }

            // Don Metzler's idf fix
            float idf = similarity.idf(collectionStats.totalTermFreq(term), collectionStats.getSumTotalTermFreq());
            fbWeight *= idf;

            f.addFeatureWeight(term, fbWeight);
        }

        f.pruneToSize(maxQueryTerms);
        f.scaleToUnitL1Norm();

        return f;
    }
}
