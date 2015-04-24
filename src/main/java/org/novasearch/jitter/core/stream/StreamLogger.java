package org.novasearch.jitter.core.stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.LevelFilter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.spi.FilterReply;
import io.dropwizard.lifecycle.Managed;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import twitter4j.RawStreamListener;

import java.util.concurrent.atomic.AtomicLong;

public class StreamLogger implements RawStreamListener, Managed {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(StreamLogger.class);
    private static final String HOUR_ROLL = ".%d{yyyy-MM-dd-HH, UTC}.gz";

    private AtomicLong counter;

    public StreamLogger(String directory) {
        counter = new AtomicLong();
        LoggerContext logCtx = (LoggerContext) LoggerFactory.getILoggerFactory();

        PatternLayoutEncoder standardEncoder = new PatternLayoutEncoder();
        standardEncoder.setContext(logCtx);
        standardEncoder.setPattern("%p  [%d{ISO8601, UTC}] %c: %m%n");
        standardEncoder.start();

        PatternLayoutEncoder simpleEncoder = new PatternLayoutEncoder();
        simpleEncoder.setContext(logCtx);
        simpleEncoder.setPattern("%m%n");
        simpleEncoder.start();

        // Filter for the statuses: we only want INFO messages
        LevelFilter filterInfo = new LevelFilter();
        filterInfo.setLevel(Level.INFO);
        filterInfo.setOnMatch(FilterReply.ACCEPT);
        filterInfo.setOnMismatch(FilterReply.DENY);
        filterInfo.start();

        // Filter for the warnings: we only want WARN messages
        LevelFilter filterWarn = new LevelFilter();
        filterWarn.setLevel(Level.WARN);
        filterWarn.setOnMatch(FilterReply.ACCEPT);
        filterWarn.setOnMismatch(FilterReply.DENY);
        filterWarn.start();


        RollingFileAppender statusesAppender = new RollingFileAppender();
        statusesAppender.setContext(logCtx);
        statusesAppender.setEncoder(simpleEncoder);
        statusesAppender.setAppend(true);
        statusesAppender.addFilter(filterInfo);

        TimeBasedRollingPolicy statusesRollingPolicy = new TimeBasedRollingPolicy();
        statusesRollingPolicy.setContext(logCtx);
        statusesRollingPolicy.setParent(statusesAppender);
        statusesRollingPolicy.setFileNamePattern(FilenameUtils.concat(directory, "statuses.log" + HOUR_ROLL));
        statusesRollingPolicy.start();

        statusesAppender.setRollingPolicy(statusesRollingPolicy);
        statusesAppender.start();


        RollingFileAppender warningsAppender = new RollingFileAppender();
        warningsAppender.setContext(logCtx);
        warningsAppender.setEncoder(standardEncoder);
        warningsAppender.setAppend(true);
        warningsAppender.addFilter(filterWarn);

        TimeBasedRollingPolicy warningsRollingPolicy = new TimeBasedRollingPolicy();
        warningsRollingPolicy.setContext(logCtx);
        warningsRollingPolicy.setParent(warningsAppender);
        warningsRollingPolicy.setFileNamePattern(FilenameUtils.concat(directory, "warnings.log" + HOUR_ROLL));
        warningsRollingPolicy.start();

        warningsAppender.setRollingPolicy(warningsRollingPolicy);
        warningsAppender.start();

        // configures the logger
        logger.setAdditive(false);
        logger.setLevel(Level.INFO);
        logger.addAppender(statusesAppender);
        logger.addAppender(warningsAppender);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public void onMessage(String rawString) {
        long cnt = counter.incrementAndGet();
        logger.info(StringUtils.chomp(rawString));
        if (cnt % 1000 == 0) {
            logger.warn(cnt + " messages received.");
        }
    }

    @Override
    public void onException(Exception ex) {
        logger.warn(ex.getMessage());
    }
}
