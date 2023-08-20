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

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KeyedBoundedOutOfOrdernessWatermarks<T, KEY>
        implements KeyedWatermarkGenerator<T, KEY> {

    /** Hashmap to store the max timestamps seen so far for each key. */
    private final ConcurrentHashMap<KEY, Long> maxTimestamps;

    /** The maximum out-of-orderness that this keyed watermark generator assumes. */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new keyed watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public KeyedBoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // To store the maxTimestamp for each key, instead of only one maxTimestamp
        // That will be used then by the onPeriodicEmit() to emit a watermark with the maxTimestamp
        // for each key
        // Default value will be added if the hashmap doesn't contain the key with : Long.MIN_VALUE
        // + outOfOrdernessMillis + 1 (onEvent())
        this.maxTimestamps = new ConcurrentHashMap<>(16, 0.9f, 1);
    }

    // ------------------------------------------------------------------------

    @Override
    public void onEvent(T event, KEY k, long eventTimestamp, KeyedWatermarkOutput output) {
        maxTimestamps.put(
                k,
                Math.max(
                        maxTimestamps.getOrDefault(k, (Long.MIN_VALUE + outOfOrdernessMillis + 1)),
                        eventTimestamp));
    }

    @Override
    public void onPeriodicEmit(KeyedWatermarkOutput output) {
        /**
         * In each call of onPeriodicEmit We need to iterate over the maxTimestamps Hashmap to emit
         * a watermark for each key with its maxTimestamp.
         */
        for (KEY k : maxTimestamps.keySet()) {
            output.emitWatermark(new Watermark(maxTimestamps.get(k) - outOfOrdernessMillis - 1), k);
        }
    }
}
