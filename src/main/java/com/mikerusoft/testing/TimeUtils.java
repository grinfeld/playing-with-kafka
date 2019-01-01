package com.mikerusoft.testing;

import lombok.Data;
import lombok.Value;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class TimeUtils {

    private TimeUtils() {}

    public static String truncate(LocalDateTime date, ChronoUnit timeUnit) {
        return InnerTimeUnit.format(date, timeUnit);
    }

    public static String extractWindowStart(long time, long windowDurationSec) {
        Instant localDateTime = Instant.ofEpochMilli(time);
        Pair<ChronoUnit, Long> unitToWindowPair = resolveChronoUnitAndWindowStart(localDateTime, windowDurationSec);
        Instant minus = localDateTime.minus(unitToWindowPair.getRight(), ChronoUnit.SECONDS);
        return truncate(LocalDateTime.ofInstant(minus, ZoneOffset.UTC), unitToWindowPair.getLeft());
    }

    public static long extractWindowStartDate(long time, long windowDurationSec) {
        Instant localDateTime = Instant.ofEpochMilli(time);
        Pair<ChronoUnit, Long> unitToWindowPair = resolveChronoUnitAndWindowStart(localDateTime, windowDurationSec);
        Instant minus = localDateTime.minus(unitToWindowPair.getRight(), ChronoUnit.SECONDS);
        return LocalDateTime.ofInstant(minus, ZoneOffset.UTC).toEpochSecond(ZoneOffset.UTC);
    }

    private static Pair<ChronoUnit, Long> resolveChronoUnitAndWindowStart(Instant instant, long windowDurationSec) {
        int fraction;
        long window;
        ChronoUnit unit;
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        if (TimeUnit.SECONDS.toDays(windowDurationSec) > 0) {
            fraction = localDateTime.get(ChronoField.DAY_OF_MONTH);
            window = TimeUnit.DAYS.toSeconds(fraction) % windowDurationSec;
            unit = ChronoUnit.DAYS;
        } else if (TimeUnit.SECONDS.toHours(windowDurationSec) > 0) {
            fraction = localDateTime.get(ChronoField.HOUR_OF_DAY);
            window = TimeUnit.HOURS.toSeconds(fraction) % windowDurationSec;
            unit = ChronoUnit.HOURS;
        } else if (TimeUnit.SECONDS.toMinutes(windowDurationSec) > 0) {
            fraction = localDateTime.get(ChronoField.MINUTE_OF_HOUR);
            window = TimeUnit.MINUTES.toSeconds(fraction) % windowDurationSec;
            unit = ChronoUnit.MINUTES;
        } else {
            fraction = localDateTime.get(ChronoField.SECOND_OF_MINUTE);
            window = fraction % windowDurationSec;
            unit = ChronoUnit.SECONDS;
        }
        return Pair.of(unit, window);
    }

    private enum InnerTimeUnit {
        SECONDS (ChronoUnit.SECONDS, "yyyy-MM-dd HH:mm:ss"),
        MINUTES (ChronoUnit.MINUTES, "yyyy-MM-dd HH:mm"),
        HOURS (ChronoUnit.HOURS, "yyyy-MM-dd HH"),
        DAYS (ChronoUnit.DAYS, "yyyy-MM-dd")
        ;

        private static InnerTimeUnit get(ChronoUnit unit) {
            return Stream.of(InnerTimeUnit.values()).filter(t -> t.unit.equals(unit)).findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Not supported time unit " + unit.name()));
        }

        private static String format(TemporalAccessor date, ChronoUnit timeUnit) {
            return get(timeUnit).format(date);
        }

        private ChronoUnit unit;
        private DateTimeFormatter formatter;

        InnerTimeUnit(ChronoUnit unit, String format) {
            this.unit = unit;
            this.formatter = DateTimeFormatter.ofPattern(format);
        }

        private String format(TemporalAccessor date) {
            return this.formatter.format(date);
        }
    }

    @Data
    @Value
    public static class Pair<L, R> {
        private L left;
        private R right;

        public static <L,R> Pair<L,R>of(L left, R right) {
            return new Pair<>(left, right);
        }

        private Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }
    }
}
