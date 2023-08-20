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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializer for {@link StreamRecord}, {@link Watermark}, {@link LatencyMarker}, and {@link
 * WatermarkStatus}.
 *
 * <p>This does not behave like a normal {@link TypeSerializer}, instead, this is only used at the
 * stream task/operator level for transmitting StreamRecords and Watermarks.
 *
 * @param <T> The type of value in the StreamRecord
 */
@Internal
public final class StreamElementSerializer<T> extends TypeSerializer<StreamElement> {

    private static final long serialVersionUID = 1L;

    private static final int TAG_REC_WITH_TIMESTAMP = 0;
    private static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
    private static final int TAG_WATERMARK = 2;
    private static final int TAG_LATENCY_MARKER = 3;
    private static final int TAG_STREAM_STATUS = 4;
    private static final int TAG_KEYED_WATERMARK = 5;
    private static final int TAG_KEYED_WATERMARK_STATUS = 6;

    // The following tags will be used in case of keyed watermarks based on the datatype of the key
    // (For Watermark)

    private static final int TAG_KEYED_LONG = 10;
    private static final int TAG_KEYED_INT = 11;
    private static final int TAG_KEYED_CHAR = 12;
    private static final int TAG_KEYED_STR = 13;
    private static final int TAG_KEYED_DOUBLE = 14;
    private static final int TAG_KEYED_FLOAT = 15;

    // The following tags will be used in case of keyed watermarks based on the datatype of the key
    // (For Watermark Status)
    private static final int TAG_STATUS_KEYED_LONG = 20;
    private static final int TAG_STATUS_KEYED_INT = 21;
    private static final int TAG_STATUS_KEYED_CHAR = 22;
    private static final int TAG_STATUS_KEYED_STR = 23;
    private static final int TAG_STATUS_KEYED_DOUBLE = 24;
    private static final int TAG_STATUS_KEYED_FLOAT = 25;

    private final TypeSerializer<T> typeSerializer;

    public StreamElementSerializer(TypeSerializer<T> serializer) {
        if (serializer instanceof StreamElementSerializer) {
            throw new RuntimeException(
                    "StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: "
                            + serializer);
        }
        this.typeSerializer = requireNonNull(serializer);
    }

    public TypeSerializer<T> getContainedTypeSerializer() {
        return this.typeSerializer;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StreamElementSerializer<T> duplicate() {
        TypeSerializer<T> copy = typeSerializer.duplicate();
        return (copy == typeSerializer) ? this : new StreamElementSerializer<T>(copy);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public StreamRecord<T> createInstance() {
        return new StreamRecord<T>(typeSerializer.createInstance());
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public StreamElement copy(StreamElement from) {
        // we can reuse the timestamp since Instant is immutable
        if (from.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            return fromRecord.copy(typeSerializer.copy(fromRecord.getValue()));
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement copy(StreamElement from, StreamElement reuse) {
        if (from.isRecord() && reuse.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            StreamRecord<T> reuseRecord = reuse.asRecord();

            T valueCopy = typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
            fromRecord.copyTo(valueCopy, reuseRecord);
            return reuse;
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int tag = source.readByte();
        target.write(tag);

        if (tag == TAG_REC_WITH_TIMESTAMP) {
            // move timestamp
            target.writeLong(source.readLong());
            typeSerializer.copy(source, target);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            typeSerializer.copy(source, target);
        } else if (tag == TAG_WATERMARK) { // Default Watermark
            target.writeLong(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) { // Default Watermark Status
            target.writeInt(source.readInt());
        } else if (tag == TAG_KEYED_LONG) { // Keyed Watermark with key of type Long
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
        } else if (tag == TAG_KEYED_INT) { // Keyed Watermark with key of type Int
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        } else if (tag == TAG_KEYED_CHAR) { // Keyed Watermark with key of type Char
            target.writeLong(source.readLong());
            target.writeChar(source.readChar());
        } else if (tag == TAG_KEYED_STR) { // Keyed Watermark with key of type String
            target.writeLong(source.readLong());
            target.writeChars(source.readLine());
        } else if (tag == TAG_KEYED_DOUBLE) { // Keyed Watermark with key of type Double
            target.writeLong(source.readLong());
            target.writeDouble(source.readDouble());
        } else if (tag == TAG_KEYED_FLOAT) { // Keyed Watermark with key of type Float
            target.writeLong(source.readLong());
            target.writeFloat(source.readFloat());
        } else if (tag == TAG_STATUS_KEYED_LONG) { // Keyed Watermark Status with Key of type Long
            target.writeInt(source.readInt());
            target.writeFloat(source.readLong());
        } else if (tag == TAG_STATUS_KEYED_INT) { // Keyed Watermark Status with Key of type Int
            target.writeInt(source.readInt());
            target.writeInt(source.readInt());
        } else if (tag == TAG_STATUS_KEYED_CHAR) { // Keyed Watermark Status with Key of type Char
            target.writeInt(source.readInt());
            target.writeChar(source.readChar());
        } else if (tag == TAG_STATUS_KEYED_STR) { // Keyed Watermark Status with Key of type String
            target.writeInt(source.readInt());
            target.writeChars(source.readLine());
        } else if (tag
                == TAG_STATUS_KEYED_DOUBLE) { // Keyed Watermark Status with Key of type Double
            target.writeInt(source.readInt());
            target.writeDouble(source.readDouble());
        } else if (tag == TAG_STATUS_KEYED_FLOAT) { // Keyed Watermark Status with Key of type Float
            target.writeInt(source.readInt());
            target.writeFloat(source.readFloat());
        } else if (tag == TAG_LATENCY_MARKER) {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public void serialize(StreamElement value, DataOutputView target) throws IOException {
        if (value.isRecord()) {
            StreamRecord<T> record = value.asRecord();
            if (record.hasTimestamp()) {
                target.write(TAG_REC_WITH_TIMESTAMP);
                target.writeLong(record.getTimestamp());
            } else {
                target.write(TAG_REC_WITHOUT_TIMESTAMP);
            }
            typeSerializer.serialize(record.getValue(), target);
        } else if (value.isWatermark()) {
            if (value.asWatermark().getKey() != null) {
                // If key is not null, write the tag, then the w ts, then the w key
                if (value.asWatermark().getKey().getClass() == Long.class) {
                    target.write(TAG_KEYED_LONG);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeLong((Long) value.asWatermark().getKey());
                } else if (value.asWatermark().getKey().getClass() == Integer.class) {
                    target.write(TAG_KEYED_INT);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeInt((Integer) value.asWatermark().getKey());
                } else if (value.asWatermark().getKey().getClass() == Character.class) {
                    target.write(TAG_KEYED_CHAR);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeChar((Character) value.asWatermark().getKey());
                } else if (value.asWatermark().getKey().getClass() == String.class) {
                    target.write(TAG_KEYED_STR);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeChars((String) value.asWatermark().getKey());
                } else if (value.asWatermark().getKey().getClass() == Double.class) {
                    target.write(TAG_KEYED_DOUBLE);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeDouble((Double) value.asWatermark().getKey());
                } else if (value.asWatermark().getKey().getClass() == Float.class) {
                    target.write(TAG_KEYED_FLOAT);
                    target.writeLong(value.asWatermark().getTimestamp());
                    target.writeFloat((Float) value.asWatermark().getKey());
                }
            } else {
                target.write(TAG_WATERMARK);
                target.writeLong(value.asWatermark().getTimestamp());
            }
        } else if (value.isWatermarkStatus()) {
            if (value.asWatermarkStatus().getKey() != null) {
                // If key is not null, write the tag, then the w status, then the w key
                if (value.asWatermarkStatus().getKey().getClass() == Long.class) {
                    target.write(TAG_STATUS_KEYED_LONG);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeLong((Long) value.asWatermarkStatus().getKey());
                } else if (value.asWatermarkStatus().getKey().getClass() == Integer.class) {
                    target.write(TAG_STATUS_KEYED_INT);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeInt((Integer) value.asWatermarkStatus().getKey());
                } else if (value.asWatermarkStatus().getKey().getClass() == Character.class) {
                    target.write(TAG_STATUS_KEYED_CHAR);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeChar((Character) value.asWatermarkStatus().getKey());
                } else if (value.asWatermarkStatus().getKey().getClass() == String.class) {
                    target.write(TAG_STATUS_KEYED_STR);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeChars((String) value.asWatermarkStatus().getKey());
                } else if (value.asWatermarkStatus().getKey().getClass() == Double.class) {
                    target.write(TAG_STATUS_KEYED_DOUBLE);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeDouble((Double) value.asWatermarkStatus().getKey());
                } else if (value.asWatermarkStatus().getKey().getClass() == Float.class) {
                    target.write(TAG_STATUS_KEYED_FLOAT);
                    target.writeInt(value.asWatermarkStatus().getStatus());
                    target.writeFloat((Float) value.asWatermarkStatus().getKey());
                }
            } else {
                target.write(TAG_STREAM_STATUS);
                target.writeInt(value.asWatermarkStatus().getStatus());
            }
        } else if (value.isLatencyMarker()) {
            target.write(TAG_LATENCY_MARKER);
            target.writeLong(value.asLatencyMarker().getMarkedTime());
            target.writeLong(value.asLatencyMarker().getOperatorId().getLowerPart());
            target.writeLong(value.asLatencyMarker().getOperatorId().getUpperPart());
            target.writeInt(value.asLatencyMarker().getSubtaskIndex());
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement deserialize(DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            return new StreamRecord<T>(typeSerializer.deserialize(source));
        } else if (tag == TAG_WATERMARK) { // Default Watermark
            return new Watermark(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) { // Default Watermark Status
            return new WatermarkStatus(source.readInt());
        } else if (tag == TAG_KEYED_LONG) { // Keyed Watermark with Key of type Long
            return new Watermark(source.readLong(), source.readLong());
        } else if (tag == TAG_KEYED_INT) { // Keyed Watermark with Key of type Int
            return new Watermark(source.readLong(), source.readInt());
        } else if (tag == TAG_KEYED_CHAR) { // Keyed Watermark with Key of type Char
            return new Watermark(source.readLong(), source.readChar());
        } else if (tag == TAG_KEYED_STR) { // Keyed Watermark with Key of type String
            return new Watermark(source.readLong(), source.readLine());
        } else if (tag == TAG_KEYED_DOUBLE) { // Keyed Watermark with Key of type Double
            return new Watermark(source.readLong(), source.readDouble());
        } else if (tag == TAG_KEYED_FLOAT) { // Keyed Watermark with Key of type Float
            return new Watermark(source.readLong(), source.readFloat());
        } else if (tag == TAG_STATUS_KEYED_LONG) { // Keyed Watermark Status with Key of type Long
            return new WatermarkStatus(source.readInt(), source.readLong());
        } else if (tag == TAG_STATUS_KEYED_INT) { // Keyed Watermark Status with Key of type Int
            return new WatermarkStatus(source.readInt(), source.readInt());
        } else if (tag == TAG_STATUS_KEYED_CHAR) { // Keyed Watermark Status with Key of type Char
            return new WatermarkStatus(source.readInt(), source.readChar());
        } else if (tag == TAG_STATUS_KEYED_STR) { // Keyed Watermark Status with Key of type String
            return new WatermarkStatus(source.readInt(), source.readLine());
        } else if (tag
                == TAG_STATUS_KEYED_DOUBLE) { // Keyed Watermark Status with Key of type Double
            return new WatermarkStatus(source.readInt(), source.readDouble());
        } else if (tag == TAG_STATUS_KEYED_FLOAT) { // Keyed Watermark Status with Key of type Float
            return new WatermarkStatus(source.readInt(), source.readFloat());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value, timestamp);
            return reuseRecord;
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value);
            return reuseRecord;
        } else if (tag == TAG_WATERMARK) { // Default Watermark
            return new Watermark(source.readLong());
        } else if (tag == TAG_KEYED_LONG) { // Keyed Watermark with Key of type Long
            return new Watermark(source.readLong(), source.readLong());
        } else if (tag == TAG_KEYED_INT) { // Keyed Watermark with Key of type Int
            return new Watermark(source.readLong(), source.readInt());
        } else if (tag == TAG_KEYED_CHAR) { // Keyed Watermark with Key of type Char
            return new Watermark(source.readLong(), source.readChar());
        } else if (tag == TAG_KEYED_STR) { // Keyed Watermark with Key of type String
            return new Watermark(source.readLong(), source.readLine());
        } else if (tag == TAG_KEYED_DOUBLE) { // Keyed Watermark with Key of type Double
            return new Watermark(source.readLong(), source.readDouble());
        } else if (tag == TAG_KEYED_FLOAT) { // Keyed Watermark with Key of type Float
            return new Watermark(source.readLong(), source.readFloat());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamElementSerializer) {
            StreamElementSerializer<?> other = (StreamElementSerializer<?>) obj;

            return typeSerializer.equals(other.typeSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return typeSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    //
    // This serializer may be used by Flink internal operators that need to checkpoint
    // buffered records. Therefore, it may be part of managed state and need to implement
    // the configuration snapshot and compatibility methods.
    // --------------------------------------------------------------------------------------------

    @Override
    public StreamElementSerializerSnapshot<T> snapshotConfiguration() {
        return new StreamElementSerializerSnapshot<>(this);
    }

    /**
     * Configuration snapshot specific to the {@link StreamElementSerializer}.
     *
     * @deprecated see {@link StreamElementSerializerSnapshot}.
     */
    @Deprecated
    public static final class StreamElementSerializerConfigSnapshot<T>
            extends CompositeTypeSerializerConfigSnapshot<StreamElement> {

        private static final int VERSION = 1;

        /** This empty nullary constructor is required for deserializing the configuration. */
        public StreamElementSerializerConfigSnapshot() {}

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public TypeSerializerSchemaCompatibility<StreamElement> resolveSchemaCompatibility(
                TypeSerializer<StreamElement> newSerializer) {
            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer,
                    new StreamElementSerializerSnapshot<>(),
                    getSingleNestedSerializerAndConfig().f1);
        }
    }

    /** Configuration snapshot specific to the {@link StreamElementSerializer}. */
    public static final class StreamElementSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<StreamElement, StreamElementSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings("WeakerAccess")
        public StreamElementSerializerSnapshot() {
            super(StreamElementSerializer.class);
        }

        StreamElementSerializerSnapshot(StreamElementSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StreamElementSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.getContainedTypeSerializer()};
        }

        @Override
        protected StreamElementSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> casted = (TypeSerializer<T>) nestedSerializers[0];

            return new StreamElementSerializer<>(casted);
        }
    }
}
