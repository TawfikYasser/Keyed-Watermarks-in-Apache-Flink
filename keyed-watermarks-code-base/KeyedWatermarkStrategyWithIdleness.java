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

final class KeyedWatermarkStrategyWithIdleness<T, KEY> implements KeyedWatermarkStrategy<T, KEY> {
    private static final long serialVersionUID = 1L;

    private final KeyedWatermarkStrategy<T, KEY> baseStrategy;
    private final Duration idlenessTimeout;

    KeyedWatermarkStrategyWithIdleness(
            KeyedWatermarkStrategy<T, KEY> baseStrategy, Duration idlenessTimeout) {
        this.baseStrategy = baseStrategy;
        this.idlenessTimeout = idlenessTimeout;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return baseStrategy.createTimestampAssigner(context);
    }

    @Override
    public KeyedWatermarkGenerator<T, KEY> createKeyedWatermarkGenerator(
            KeyedWatermarkGeneratorSupplier.Context context) {
        return new KeyedWatermarksWithIdleness<>(
                baseStrategy.createKeyedWatermarkGenerator(context), idlenessTimeout);
    }

    @Override
    public WatermarkAlignmentParams getAlignmentParameters() {
        return baseStrategy.getAlignmentParameters();
    }
}
