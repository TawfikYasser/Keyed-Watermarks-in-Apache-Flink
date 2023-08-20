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

final class KeyedWatermarksWithWatermarkAlignment<T, KEY>
        implements KeyedWatermarkStrategy<T, KEY> {
    static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofMillis(1000);

    private final KeyedWatermarkStrategy<T, KEY> strategy;

    private final String watermarkGroup;

    private final Duration maxAllowedWatermarkDrift;

    private final Duration updateInterval;

    public KeyedWatermarksWithWatermarkAlignment(
            KeyedWatermarkStrategy<T, KEY> strategy,
            String watermarkGroup,
            Duration maxAllowedWatermarkDrift,
            Duration updateInterval) {
        this.strategy = strategy;
        this.watermarkGroup = watermarkGroup;
        this.maxAllowedWatermarkDrift = maxAllowedWatermarkDrift;
        this.updateInterval = updateInterval;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return strategy.createTimestampAssigner(context);
    }

    @Override
    public KeyedWatermarkGenerator<T, KEY> createKeyedWatermarkGenerator(
            KeyedWatermarkGeneratorSupplier.Context context) {
        return strategy.createKeyedWatermarkGenerator(context);
    }

    @Override
    public WatermarkAlignmentParams getAlignmentParameters() {
        return new WatermarkAlignmentParams(
                maxAllowedWatermarkDrift.toMillis(), watermarkGroup, updateInterval.toMillis());
    }
}
