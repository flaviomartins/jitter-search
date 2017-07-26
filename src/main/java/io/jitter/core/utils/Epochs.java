package io.jitter.core.utils;

import org.joda.time.DateTime;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.joda.time.DateTimeConstants.MILLIS_PER_SECOND;

public class Epochs {

    private static final Pattern RANGE_PATTERN;

    static {
        synchronized (Epochs.class) {
            RANGE_PATTERN = Pattern.compile("[: ]");
        }
    }

    public static long[] parseEpoch(Optional<String> epoch) {
        long[] epochs = new long[2];
        if (epoch.isPresent()) {
            epochs = Epochs.parseEpochRange(epoch.get());
        }
        return epochs;
    }

    public static long[] parseEpochRange(String epochRange) {
        long[] epochs = new long[]{0L, Long.MAX_VALUE};
        String[] split = RANGE_PATTERN.split(epochRange);
        try {
            if (split.length == 1) {
                if (epochRange.endsWith(":")) {
                    epochs[0] = Long.parseLong(split[0]);
                } else {
                    epochs[1] = Long.parseLong(split[0]);
                } 
            } else {
                epochs[0] = Long.parseLong(split[0]);
                epochs[1] = Long.parseLong(split[1]);
            }
        } catch (Exception e) {
            // pass
        }
        return epochs;
    }

    public static long[] parseDay(DateTime dateTime) {
        long[] epochs = new long[]{0L, Long.MAX_VALUE};
        DateTime startDateTime = dateTime.withTimeAtStartOfDay();
        DateTime endDateTime = dateTime.millisOfDay().withMaximumValue();
        epochs[0] = startDateTime.getMillis() / MILLIS_PER_SECOND;
        epochs[1] = endDateTime.getMillis() / MILLIS_PER_SECOND;
        return epochs;
    }
}
