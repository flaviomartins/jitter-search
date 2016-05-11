package io.jitter.core.utils;

import java.util.Optional;

public class Epochs {

    public static long[] parseEpoch(Optional<String> epoch) {
        long[] epochs = new long[2];
        if (epoch.isPresent()) {
            epochs = Epochs.parseEpochRange(epoch.get());
        }
        return epochs;
    }

    public static long[] parseEpochRange(String epochRange) {
        long[] epochs = new long[]{0L, Long.MAX_VALUE};
        String[] split = epochRange.split("[: ]");
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
}
