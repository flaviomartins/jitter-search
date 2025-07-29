package io.jitter.core.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_SECOND;


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

    public static long[] parseDay(LocalDateTime dateTime) {
        long[] epochs = new long[]{0L, Long.MAX_VALUE};
        LocalDateTime startDateTime = dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime endDateTime = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
        epochs[0] = startDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() / MILLIS_PER_SECOND;
        epochs[1] = endDateTime.toInstant(ZoneOffset.UTC).toEpochMilli() / MILLIS_PER_SECOND;
        return epochs;
    }
}
