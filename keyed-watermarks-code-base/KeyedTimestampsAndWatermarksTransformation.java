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
package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.eventtime.KeyedWatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KeyedTimestampsAndWatermarksTransformation<IN, KEY>
        extends PhysicalTransformation<IN> {

    private final Transformation<IN> input;
    private final KeyedWatermarkStrategy<IN, KEY> keyedWatermarkStrategy;

    /**
     * The key selector that can get the key by which the stream if partitioned from the elements.
     */
    private final KeySelector<IN, KEY> keySelector;

    /** The type of the key by which the stream is partitioned. */
    private final TypeInformation<KEY> keyType;

    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param parallelism The parallelism of this {@code Transformation}
     * @param input The input transformation of this {@code Transformation}
     * @param keyedWatermarkStrategy The {@link KeyedWatermarkStrategy} to use
     */
    public KeyedTimestampsAndWatermarksTransformation(
            String name,
            int parallelism,
            Transformation<IN> input,
            KeyedWatermarkStrategy<IN, KEY> keyedWatermarkStrategy,
            TypeInformation<KEY> keyType,
            KeySelector<IN, KEY> keySelector) {
        super(name, input.getOutputType(), parallelism);
        this.input = input;
        this.keyedWatermarkStrategy = keyedWatermarkStrategy;
        this.keySelector = keySelector;
        this.keyType = keyType;
    }

    public KeySelector<IN, KEY> getKeySelector() {
        return keySelector;
    }

    public TypeInformation<KEY> getKeyType() {
        return keyType;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
    }

    /** Returns the {@code WatermarkStrategy} to use. */
    public KeyedWatermarkStrategy<IN, KEY> getKeyedWatermarkStrategy() {
        return keyedWatermarkStrategy;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> transformations = Lists.newArrayList();
        transformations.add(this);
        transformations.addAll(input.getTransitivePredecessors());
        return transformations;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy chainingStrategy) {
        this.chainingStrategy = checkNotNull(chainingStrategy);
    }
}
