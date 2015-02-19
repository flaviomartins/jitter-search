package org.novasearch.jitter.core.twitter.archiver;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Object representing a status.
 * format: <id> <date> <<screen_name>> <tweet_text>
 * Date format is: YYYY-MM-DD HH:MM:SS TZ.
 */
public class Status {
    private static final Logger logger = Logger.getLogger(Status.class);

    private static final String DATE_FORMAT = "yyyy-M-d k:m:s z"; //"2014-11-25 18:31:40 WET";

    private long id;
    private String createdAt;
    private String screenName;
    private long epoch;
    private String text;

    protected Status() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public static Status fromText(String raw) {
        Status status = new Status();

        String[] split = raw.trim().split(" ", 2);
        long tid = Long.parseLong(split[0]);
        int first = split[1].indexOf("<");
        int last = split[1].indexOf(">", first);

        status.id = tid;
        status.createdAt = split[1].substring(0, first).trim();
        status.screenName = split[1].substring(first + 1, last);
        status.text = split[1].substring(last + 1).trim();

        try {
            status.epoch = (new SimpleDateFormat(DATE_FORMAT)).parse(status.createdAt).getTime() / 1000;
        } catch (ParseException e) {
            status.epoch = -1L;
        }

        return status;
    }
}
