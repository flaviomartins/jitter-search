package org.novasearch.jitter.core.twitter.archiver;

import com.google.common.base.Preconditions;

import java.io.*;

/**
 * Abstraction to read tweets in the format of twitter-archiver.
 * Tweets of users, sorted by date from oldest to newest, one per line
 * in the following format: <id> <date> <<screen_name>> <tweet_text>
 * Date format is: YYYY-MM-DD HH:MM:SS TZ.
 */
public class FileStatusReader {
    private final BufferedReader br;

    public FileStatusReader(File file) throws IOException {
        Preconditions.checkNotNull(file);

        br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
    }

    /**
     * Returns the next status, or <code>null</code> if no more statuses.
     */
    public Status next() throws IOException {
        Status nxt = null;
        String raw = null;
        while (nxt == null) {
            raw = br.readLine();

            // Check to see if we've reached end of file.
            if (raw == null) {
                return null;
            }

            nxt = Status.fromText(raw);
        }
        return Status.fromText(raw);
    }

    public void close() throws IOException {
        br.close();
    }

}
