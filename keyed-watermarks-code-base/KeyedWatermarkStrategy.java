/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Experimental;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public interface KeyedWatermarkStrategy<T, KEY>
        extends TimestampAssignerSupplier<T>, KeyedWatermarkGeneratorSupplier<T, KEY> {
    // ------------------------------------------------------------------------
    //  Methods that implementors need to implement.
    // ------------------------------------------------------------------------

    /**
     * Instantiates a KeyedWatermarkGenerator that generates watermarks according to this strategy.
     */
    @Override
    KeyedWatermarkGenerator<T, KEY> createKeyedWatermarkGenerator(
            KeyedWatermarkGeneratorSupplier.Context context);

    /**
     * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this strategy.
     */
    @Override
    default TimestampAssigner<T> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        // By default, this is {@link RecordTimestampAssigner},
        // for cases where records come out of a source with valid timestamps, for example from
        // Kafka.
        return new RecordTimestampAssigner<>();
    }

    /**
     * Provides configuration for watermark alignment of a maximum watermark of multiple
     * sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     */
    @Experimental
    default WatermarkAlignmentParams getAlignmentParameters() {
        return WatermarkAlignmentParams.WATERMARK_ALIGNMENT_DISABLED;
    }

    // ------------------------------------------------------------------------
    //  Builder methods for enriching a base WatermarkStrategy
    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}).
     *
     * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
     * access to the metrics system.
     *
     * <pre>
     * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategy
     *   .forMonotonousTimestamps()
     *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx));
     * }</pre>
     */
    default KeyedWatermarkStrategy<T, KEY> withTimestampAssigner(
            TimestampAssignerSupplier<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new KeyedWatermarkStrategyWithTimestampAssigner<>(this, timestampAssigner);
    }

    /**
     * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
     * {@link SerializableTimestampAssigner}.
     *
     * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
     * function.
     *
     * <pre>
     * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategy
     *   .<CustomObject>forMonotonousTimestamps()
     *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
     * }</pre>
     */
    default KeyedWatermarkStrategy<T, KEY> withTimestampAssigner(
            SerializableTimestampAssigner<T> timestampAssigner) {
        checkNotNull(timestampAssigner, "timestampAssigner");
        return new KeyedWatermarkStrategyWithTimestampAssigner<>(
                this, TimestampAssignerSupplier.of(timestampAssigner));
    }

    /**
     * Creates a new enriched {@link KeyedWatermarkStrategy} that also does idleness detection in
     * the created {@link KeyedWatermarkGenerator}.
     *
     * <p>Add an idle timeout to the watermark strategy. If no records flow in a partition of a
     * stream for that amount of time, then that partition is considered "idle" and will not hold
     * back the progress of watermarks in downstream operators.
     *
     * <p>Idleness can be important if some partitions have little data and might not have events
     * during some periods. Without idleness, these streams can stall the overall event time
     * progress of the application.
     */
    default KeyedWatermarkStrategy<T, KEY> withIdleness(Duration idleTimeout) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        return new KeyedWatermarkStrategyWithIdleness<>(this, idleTimeout);
    }

    /**
     * Creates a new {@link KeyedWatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     */
    @Experimental
    default KeyedWatermarkStrategy<T, KEY> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift) {
        return withWatermarkAlignment(
                watermarkGroup,
                maxAllowedWatermarkDrift,
                WatermarksWithWatermarkAlignment.DEFAULT_UPDATE_INTERVAL);
    }

    /**
     * Creates a new {@link KeyedWatermarkStrategy} that configures the maximum watermark drift from
     * other sources/tasks/partitions in the same watermark group. The group may contain completely
     * independent sources (e.g. File and Kafka).
     *
     * <p>Once configured Flink will "pause" consuming from a source/task/partition that is ahead of
     * the emitted watermark in the group by more than the maxAllowedWatermarkDrift.
     *
     * @param watermarkGroup A group of sources to align watermarks
     * @param maxAllowedWatermarkDrift Maximal drift, before we pause consuming from the
     *     source/task/partition
     * @param updateInterval How often tasks should notify coordinator about the current watermark
     *     and how often the coordinator should announce the maximal aligned watermark.
     */
    @Experimental
    default KeyedWatermarkStrategy<T, KEY> withWatermarkAlignment(
            String watermarkGroup, Duration maxAllowedWatermarkDrift, Duration updateInterval) {
        return new KeyedWatermarksWithWatermarkAlignment<T, KEY>(
                this, watermarkGroup, maxAllowedWatermarkDrift, updateInterval);
    }

    // ------------------------------------------------------------------------
    //  Convenience methods for common watermark strategies
    // ------------------------------------------------------------------------

    /**
     * Creates a watermark strategy for situations with monotonously ascending timestamps.
     *
     * <p>The watermarks are generated periodically and tightly follow the latest timestamp in the
     * data. The delay introduced by this strategy is mainly the periodic interval in which the
     * watermarks are generated.
     *
     * @see AscendingTimestampsWatermarks
     */
    static <T, KEY> KeyedWatermarkStrategy<T, KEY> forMonotonousTimestamps() {
        return (ctx) -> new KeyedAscendingTimestampsWatermarks<>();
    }

    /**
     * Creates a watermark strategy for situations where records are out of order, but you can place
     * an upper bound on how far the events are out of order. An out-of-order bound B means that
     * once the an event with timestamp T was encountered, no events older than {@code T - B} will
     * follow any more.
     *
     * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
     * is the periodic interval length, plus the out of orderness bound.
     *
     * @see KeyedBoundedOutOfOrdernessWatermarks
     */
    static <T, KEY> KeyedWatermarkStrategy<T, KEY> forBoundedOutOfOrderness(
            Duration maxOutOfOrderness) {
        return (ctx) -> new KeyedBoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
    }

    /**
     * Creates a watermark strategy based on an existing {@link KeyedWatermarkGeneratorSupplier}.
     */
    static <T, KEY> KeyedWatermarkStrategy<T, KEY> forGenerator(
            KeyedWatermarkGeneratorSupplier<T, KEY> generatorSupplier) {
        return generatorSupplier::createKeyedWatermarkGenerator;
    }

    /**
     * Creates a watermark strategy that generates no watermarks at all. This may be useful in
     * scenarios that do pure processing-time based stream processing.
     */
    static <T, KEY> KeyedWatermarkStrategy<T, KEY> noWatermarks() {
        return (ctx) -> new KeyedNoWatermarkGenerator<>();
    }
}
