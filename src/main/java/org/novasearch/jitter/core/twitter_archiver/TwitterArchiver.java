package org.novasearch.jitter.core.twitter_archiver;

import io.dropwizard.lifecycle.Managed;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TwitterArchiver implements Managed {
    final static Logger logger = LoggerFactory.getLogger(TwitterArchiver.class);

    private final String directory;
    private final List<String> screenNames;

    private final Map<String, UserTimeline> userTimelines;

    public TwitterArchiver(String directory, List<String> screenNames) {
        this.directory = directory;
        this.screenNames = screenNames;
        this.userTimelines = new LinkedHashMap<>();
    }

    @Override
    public void start() throws Exception {
        load();
    }

    @Override
    public void stop() throws Exception {

    }

    public UserTimeline getUserTimeline(String screenName) {
        return userTimelines.get(screenName);
    }

    public void loadFromFile(String screenName) throws IOException {
        FileStatusReader fileStatusReader = new FileStatusReader(new File(FilenameUtils.concat(directory, screenName.toLowerCase())));

        UserTimeline timeline;
        if (userTimelines.get(screenName) != null) {
            timeline = userTimelines.get(screenName);
        } else {
            timeline = new UserTimeline(screenName);
            userTimelines.put(screenName, timeline);
        }

        Status status;
        int cnt = 0;
        while (true) {
            status = fileStatusReader.next();
            if (status == null) {
                break;
            }
            if (status.getId() <= timeline.getLatestId()) {
                continue;
            }
            timeline.add(status);
            cnt++;
        }
        fileStatusReader.close();
        if (cnt > 0) {
            logger.info("loaded " + cnt + " " + screenName.toLowerCase());
        }
    }

    public void load() throws IOException {
        logger.info("Loading...");
        for (String screenName : screenNames) {
            loadFromFile(screenName);
        }
    }

    public List<String> getUsers() {
        return screenNames;
    }

}
