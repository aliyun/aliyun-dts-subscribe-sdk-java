package com.taobao.drc.client.network.congestion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yangyang
 * @since 17/1/19
 */
public class ThresholdBasedCongestionController implements CongestionController {
    private static final Log logger = LogFactory.getLog(ThresholdBasedCongestionController.class);

    private final long lowerBound;
    private final long upperBound;
    private final AtomicLong counter = new AtomicLong(0);

    public ThresholdBasedCongestionController(long lowerBound, long upperBound) {
        if (lowerBound < 0 || upperBound < 0 || lowerBound > upperBound) {
            throw new IllegalArgumentException("Invalid bounds: lower [" + lowerBound + "], upper [" + upperBound + "]");
        }
        logger.info("Congestion control thresholds: lower [" + lowerBound + "], upper [" + upperBound + "]");
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public boolean produced() {
        return counter.incrementAndGet() >= upperBound;
    }

    @Override
    public boolean consumed() {
        return counter.decrementAndGet() <= lowerBound;
    }

    @Override
    public long pendingNum() {
        return counter.get();
    }
}
