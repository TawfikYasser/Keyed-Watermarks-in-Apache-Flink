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
package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedTimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.runtime.operators.KeyedTimestampsAndWatermarksOperator;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link KeyedTimestampsAndWatermarksTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to
 *     translate.
 * @param <KEY> The type of the key of the sub stream
 */
public class KeyedTimestampsAndWatermarksTransformationTranslator<IN, KEY>
        extends AbstractOneInputTransformationTranslator<
                IN, IN, KeyedTimestampsAndWatermarksTransformation<IN, KEY>> {
    @Override
    protected Collection<Integer> translateForBatchInternal(
            final KeyedTimestampsAndWatermarksTransformation<IN, KEY> transformation,
            final Context context) {
        return translateInternal(
                transformation, context, false /* don't emit progressive watermarks */);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final KeyedTimestampsAndWatermarksTransformation<IN, KEY> transformation,
            final Context context) {
        return translateInternal(transformation, context, true /* emit progressive watermarks */);
    }

    private Collection<Integer> translateInternal(
            final KeyedTimestampsAndWatermarksTransformation<IN, KEY> transformation,
            final Context context,
            boolean emitProgressiveWatermarks) {
        checkNotNull(transformation);
        checkNotNull(context);

        KeyedTimestampsAndWatermarksOperator<IN, KEY> operator =
                new KeyedTimestampsAndWatermarksOperator<>(
                        transformation.getKeyedWatermarkStrategy(), emitProgressiveWatermarks);
        SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(operator);
        operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
        return translateInternal(
                transformation,
                operatorFactory,
                transformation.getInputType(),
                transformation.getKeySelector(),
                transformation.getKeyType(),
                context);
    }
}
