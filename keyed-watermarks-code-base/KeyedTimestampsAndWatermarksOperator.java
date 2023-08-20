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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.KeyedNoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.KeyedWatermarkGenerator;
import org.apache.flink.api.common.eventtime.KeyedWatermarkOutput;
import org.apache.flink.api.common.eventtime.KeyedWatermarkStrategy;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that may do one or both of the following: extract timestamps from events and
 * generate watermarks.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones, because
 * the implementation of the timestamp assigner and the watermark generator is frequently in the
 * same class (and should be run in the same instance), even though the separate interfaces support
 * the use of different classes.
 *
 * @param <T> The type of the input elements
 * @param <KEY> The type of key related to the input elements
 */
public class KeyedTimestampsAndWatermarksOperator<T, KEY> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, ProcessingTimeService.ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final KeyedWatermarkStrategy<T, KEY> keyedWatermarkStrategy;

    /** The timestamp assigner. */
    private transient TimestampAssigner<T> timestampAssigner;

    /** The keyed watermark generator, initialized during runtime. */
    private transient KeyedWatermarkGenerator<T, KEY> keyedWatermarkGenerator;

    /** The watermark output gateway, initialized during runtime. */
    private transient KeyedWatermarkOutput wmOutput;

    /** The interval (in milliseconds) for periodic watermark probes. Initialized during runtime. */
    private transient long watermarkInterval;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    public KeyedTimestampsAndWatermarksOperator(
            KeyedWatermarkStrategy<T, KEY> keyedWatermarkStrategy,
            boolean emitProgressiveWatermarks) {
        this.keyedWatermarkStrategy = checkNotNull(keyedWatermarkStrategy);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
    }

    @Override
    public void open() throws Exception {
        super.open();

        timestampAssigner = keyedWatermarkStrategy.createTimestampAssigner(this::getMetricGroup);
        keyedWatermarkGenerator =
                emitProgressiveWatermarks
                        ? keyedWatermarkStrategy.createKeyedWatermarkGenerator(this::getMetricGroup)
                        : new KeyedNoWatermarksGenerator<>();

        wmOutput = new KeyedWatermarkEmitter(output);

        watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        if (watermarkInterval > 0 && emitProgressiveWatermarks) {
            final long now = getProcessingTimeService().getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(now + watermarkInterval, this);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(final StreamRecord<T> element) throws Exception {
        final T event = element.getValue();
        final KEY key = this.<KEY>getKeyedStateBackend().getCurrentKey();
        final long previousTimestamp =
                element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
        final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);
        element.setTimestamp(newTimestamp);
        output.collect(element);
        keyedWatermarkGenerator.onEvent(event, key, newTimestamp, wmOutput);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        keyedWatermarkGenerator.onPeriodicEmit(wmOutput);

        final long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from upstream,
     * except for the "end of time" watermark.
     */
    @Override
    public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark)
            throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE) {
            wmOutput.emitWatermark(Watermark.MAX_WATERMARK, Long.MIN_VALUE);
        }
    }

    /** Override the base implementation to completely ignore statuses propagated from upstream. */
    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}

    @Override
    public void finish() throws Exception {
        super.finish();
        keyedWatermarkGenerator.onPeriodicEmit(wmOutput);
    }

    // ------------------------------------------------------------------------

    /**
     * Implementation of the {@code KeyedWatermarkEmitter}, based on the components that are
     * available inside a stream operator.
     */
    public static final class KeyedWatermarkEmitter<T, KEY> implements KeyedWatermarkOutput<KEY> {

        private final Output<?> output;

        private boolean idle;

        /** Hashmap to store current watermarks & and their related keys */
        private final ConcurrentHashMap<Object, Long> keysAndWatermarks;

        FileHandler fileHandler;


        public KeyedWatermarkEmitter(Output<?> output) throws IOException {
            this.output = output;
            this.keysAndWatermarks = new ConcurrentHashMap<>(16, 0.9f, 1);
//            fileHandler = new FileHandler("/home/tawfekyassertawfek/Experiments/startLatencyKeyed.csv", 0, 1, true);
//            fileHandler.setFormatter(new MyFormatter());
//            Logger.getLogger("").addHandler(fileHandler);
        }

        @Override
        public void emitWatermark(Watermark watermark, Object key) {
            if (key != null) {
                if (watermark.getTimestamp()
                        <= keysAndWatermarks.getOrDefault(key, Long.MIN_VALUE)) {
                    return;
                }
                keysAndWatermarks.put(key, watermark.getTimestamp());
                markActive(key); // mark the keyed watermark as active
                // Emit the keyed watermark
//                fileHandler.publish(new LogRecord(Level.ALL, "START,"+watermark.getTimestamp()+","+key+","+System.nanoTime()));
                output.emitWatermark(
                        new org.apache.flink.streaming.api.watermark.Watermark(
                                watermark.getTimestamp(), key));
            }
        }

        @Override
        public void markIdle(Object key) {
            if (!idle) {
                idle = true;
                output.emitWatermarkStatus(
                        new org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus(
                                WatermarkStatus.IDLE_STATUS, key));
            }
        }

        @Override
        public void markActive(Object key) {
            if (idle) {
                idle = false;
                output.emitWatermarkStatus(
                        new org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus(
                                WatermarkStatus.ACTIVE_STATUS, key));
            }
        }
    }
    private static class MyFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            return record.getMessage() + "\n";
        }
    }
}
