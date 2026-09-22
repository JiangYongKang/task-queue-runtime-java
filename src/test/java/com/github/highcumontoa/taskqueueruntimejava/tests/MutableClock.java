package com.github.highcumontoa.taskqueueruntimejava.tests;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/** 测试用可控时钟：可快进时间以确定性地触发可见性超时与退避到期。 */
public final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-09-22T00:00:00Z");

    @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
    @Override public Clock withZone(ZoneId zone) { return this; }
    @Override public Instant instant() { return now; }

    public void advance(Duration d) {
        now = now.plus(d);
    }
}
