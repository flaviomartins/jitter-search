package io.jitter.core.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.collect.Lists;
import org.junit.Assert;

public class TokenizationTest extends TestCase {

    private final Object[][] examples = new Object[][] {
            {"AT&T getting secret immunity from wiretapping laws for government surveillance http://vrge.co/ZP3Fx5",
                    new String[] {"att", "get", "secret", "immun", "from", "wiretap", "law", "govern", "surveil", "http://vrge.co/ZP3Fx5"}},

            {"want to see the @verge aston martin GT4 racer tear up long beach? http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219 …",
                    new String[] {"want", "see", "@verge", "aston", "martin", "gt4", "racer", "tear", "up", "long", "beach", "http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219"}},

            {"Incredibly good news! #Drupal users rally http://bit.ly/Z8ZoFe  to ensure blind accessibility contributor gets to @DrupalCon #Opensource",
                    new String[] {"incred", "good", "new", "#drupal", "user", "ralli", "http://bit.ly/Z8ZoFe", "ensur", "blind", "access", "contributor", "get", "@drupalcon", "#opensource"}},

            {"We're entering the quiet hours at #amznhack. #Rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz",
                    new String[] {"were", "enter", "quiet", "hour", "#amznhack", "#rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz"}},

            {"The 2013 Social Event Detection Task (SED) at #mediaeval2013, http://bit.ly/16nITsf  supported by @linkedtv @project_mmixer @socialsensor_ip",
                    new String[] {"2013", "social", "event", "detect", "task", "sed", "#mediaeval2013", "http://bit.ly/16nITsf", "support", "@linkedtv", "@project_mmixer", "@socialsensor_ip"}},

            {"U.S.A. U.K. U.K USA UK #US #UK #U.S.A #U.K ...A.B.C...D..E..F..A.LONG WORD",
                    new String[] {"usa", "uk", "uk", "usa", "uk", "#us", "#uk", "#u", "sa", "#u", "k", "abc", "d", "e", "f", "long", "word"}},

            {"this is @a_valid_mention and this_is_multiple_words",
                    new String[] {"@a_valid_mention", "multipl", "word"}},

            {"PLEASE BE LOWER CASE WHEN YOU COME OUT THE OTHER SIDE - ALSO A @VALID_VALID-INVALID",
                    new String[] {"pleas", "lower", "case", "when", "you", "come", "out", "other", "side", "also", "@valid_valid", "invalid"}},

            // Note: the at sign is not the normal (at) sign and the crazy hashtag is not the normal #
            {"＠reply @with #crazy ~＃at",
                    new String[] {"＠reply", "@with", "#crazy", "＃at"}},

            {":@valid testing(valid)#hashtags. RT:@meniton (the last @mention is #valid and so is this:@valid), however this is@invalid",
                    new String[] {"@valid", "test", "valid", "#hashtags", "rt", "@meniton", "last", "@mention", "#valid", "so", "@valid", "howev", "invalid"}},

            {"this][is[lots[(of)words+with-lots=of-strange!characters?$in-fact=it&has&Every&Single:one;of<them>in_here_B&N_test_test?test\\test^testing`testing{testing}testing…testing¬testing·testing what?",
                    new String[] {"lot", "word", "lot", "strang", "charact", "fact", "ha", "everi", "singl", "on", "them", "here", "bn", "test", "test", "test", "test", "test", "test", "test", "test", "test", "test", "test", "what"}},

            {"@Porsche : 2014 is already here #zebracar #LM24 http://bit.ly/18RUczp\u00a0 pic.twitter.com/cQ7z0c2hMg",
                    new String[] {"@porsche", "2014", "alreadi", "here", "#zebracar", "#lm24", "http://bit.ly/18RUczp", "pic.twitter.com/cQ7z0c2hMg"}},

            {"Some cars are in the river #NBC4NY http://t.co/WmK9Hc…",
                    new String[] {"some", "car", "river", "#nbc4ny", "http://t.co/WmK9Hc"}},

            {"“@mention should be detected",
                    new String[] {"@mention", "should", "detect"}},

            {"Mr. Rogers's shows",
                    new String[] {"mr", "roger", "show"}},

            {"'Oz, The Great and Powerful' opens",
                    new String[] {"oz", "great", "power", "open"}},

            {"Brother of Oscar Pistorius, Carl Pistorius appears in court over road deathhttps://gu.com/p/3em7p/tw ",
                    new String[] {"brother", "oscar", "pistoriu", "carl", "pistoriu", "appear", "court", "over", "road", "death", "https://gu.com/p/3em7p/tw"}},
    };

    public void testTokenizer() throws Exception {
        Analyzer analyzer = new TweetAnalyzer();

        for (Object[] example : examples) {
            Assert.assertEquals(
                    Arrays.toString((String[]) example[1]),
                    Arrays.toString(analyze(analyzer, (String) example[0])));
        }
    }

    private static String[] analyze(Analyzer analyzer, String text) throws IOException {
        List<String> list = Lists.newArrayList();

        TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(text));
        CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            String term = cattr.toString();
            list.add(term);
        }
        tokenStream.end();
        tokenStream.close();

        return list.toArray(new String[list.size()]);
    }

}
