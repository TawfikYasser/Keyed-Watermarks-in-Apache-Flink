/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * WatermarkStatus} are propagated to downstream outputs, given a set of one or multiple input
 * channels that continuously receive them. Usages of this class need to define the number of input
 * channels that the valve needs to handle, as well as provide a implementation of {@link
 * DataOutput}, which is called by the valve only when it determines a new watermark or watermark
 * status can be propagated.
 */
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & watermark statuses are
     * fed into the valve.
     */
    private final InputChannelStatus[] channelStatuses;

    /** The last watermark emitted from the valve. */
    private long lastOutputWatermark;

    /** The last watermark emitted from the valve and its key . */
    private HashMap<Object, Long>
            lastOutputWatermarksAndKeys; // To store each lastOutputWatermark and its keys in case
    // of keyed watermarks

    /** The last watermark status emitted from the valve. */
    private WatermarkStatus lastOutputWatermarkStatus;

    /** The last watermark status emitted from the valve and its key. */
    private HashMap<Object, WatermarkStatus>
            lastOutputWatermarkStatusAndKeys; // To store each lastOutputWatermarkStatus and its
    // keys in case of keyed watermarks

    private HashMap<Object, Long> maxWatermarksAndKeys; // Will be used in
    // findAndOutputNewMinWatermarkAcrossAlignedChannels();

    private HashMap<Object, Long> newMinWatermarksAndKeys; // Will be used in
    // findAndOutputNewMinWatermarkAcrossAlignedChannels()

    private HashMap<Object, Boolean> hasAlignedChannelsKeyed; // Will be used in
    // findAndOutputNewMinWatermarkAcrossAlignedChannels()

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public StatusWatermarkValve(int numInputChannels) {
        checkArgument(numInputChannels > 0);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = Long.MIN_VALUE;
            channelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            channelStatuses[i].isWatermarkAligned = true;

            // For keyed watermarks we need to handle hashmap of keys and values for each channel.
            // for the watermark, watermarkStatus, watermarkAligned
            channelStatuses[i].keyedWatermark = new HashMap<>(); // Default value = Long.MIN_VALUE
            channelStatuses[i].keyedWatermarkStatus =
                    new HashMap<>(); // Default value = WatermarkStatus.ACTIVE
            channelStatuses[i].keyedWatermarkAligned = new HashMap<>(); // Default value = true
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;

        // For Keyed Watermarks across all channels
        this.lastOutputWatermarksAndKeys = new HashMap<>(); // Default Value = Long.MIN_VALUE
        this.lastOutputWatermarkStatusAndKeys =
                new HashMap<>(); // Default value = WatermarkStatus.ACTIVE
        this.maxWatermarksAndKeys = new HashMap<>(); // Default value = Long.MIN_VALUE
        this.newMinWatermarksAndKeys = new HashMap<>(); // Default value = Long.MAX_VALUE
        this.hasAlignedChannelsKeyed = new HashMap<>(); // Default value = false
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        // If the key related with the watermark is not null, we will work on keyed watermarks else
        // work as regular
        if (watermark.getKey() != null) {
            // ignore the input watermark if its input channel, or all input channels are idle (i.e.
            // overall the valve is idle).
            if (lastOutputWatermarkStatusAndKeys
                            .getOrDefault(watermark.getKey(), WatermarkStatus.ACTIVE)
                            .isActive()
                    && channelStatuses[channelIndex]
                            .keyedWatermarkStatus
                            .getOrDefault(watermark.getKey(), WatermarkStatus.ACTIVE)
                            .isActive()) {
                long watermarkMillis = watermark.getTimestamp();
                // if the input watermark's value is less than the last received watermark for its
                // input
                // channel, ignore it also.
                if (watermarkMillis
                        > channelStatuses[channelIndex].keyedWatermark.getOrDefault(
                                watermark.getKey(), Long.MIN_VALUE)) {
                    channelStatuses[channelIndex].keyedWatermark.put(
                            watermark.getKey(), watermarkMillis);
                    // previously unaligned input channels are now aligned if its watermark
                    // has
                    // caught
                    // up
                    if (!channelStatuses[channelIndex].keyedWatermarkAligned.getOrDefault(
                                    watermark.getKey(), true)
                            && watermarkMillis
                                    >= lastOutputWatermarksAndKeys.getOrDefault(
                                            watermark.getKey(), Long.MIN_VALUE)) {
                        channelStatuses[channelIndex].keyedWatermarkAligned.put(
                                watermark.getKey(), true);
                    }
                    // now, attempt to find a new min watermark across all aligned channels
                    findAndOutputNewMinWatermarkAcrossAlignedChannels(output, watermark.getKey());
                }
            }
        } else {
            // ignore the input watermark if its input channel, or all input channels are idle (i.e.
            // overall the valve is idle).
            if (lastOutputWatermarkStatus.isActive()
                    && channelStatuses[channelIndex].watermarkStatus.isActive()) {
                long watermarkMillis = watermark.getTimestamp();

                // if the input watermark's value is less than the last received watermark for its
                // input
                // channel, ignore it also.
                if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                    channelStatuses[channelIndex].watermark = watermarkMillis;
                    // previously unaligned input channels are now aligned if its watermark has
                    // caught
                    // up
                    if (!channelStatuses[channelIndex].isWatermarkAligned
                            && watermarkMillis >= lastOutputWatermark) {
                        channelStatuses[channelIndex].isWatermarkAligned = true;
                    }

                    // now, attempt to find a new min watermark across all aligned channels
                    findAndOutputNewMinWatermarkAcrossAlignedChannels(output, null);
                }
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link DataOutput#emitWatermarkStatus(WatermarkStatus)} will
     * be called, or a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be
     * called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus, int channelIndex, DataOutput<?> output)
            throws Exception {
        if (watermarkStatus.getKey() != null) { // In case of keyed watermarks
            // only account for watermark status inputs that will result in a status change for the
            // input
            // channel
            if (watermarkStatus.isIdle()
                    && channelStatuses[channelIndex]
                            .keyedWatermarkStatus
                            .getOrDefault(watermarkStatus.getKey(), WatermarkStatus.ACTIVE)
                            .isActive()) {
                // handle active -> idle toggle for the input channel
                channelStatuses[channelIndex].keyedWatermarkStatus.put(
                        watermarkStatus.getKey(), WatermarkStatus.IDLE);

                // the channel is now idle, therefore not aligned
                channelStatuses[channelIndex].keyedWatermarkAligned.put(
                        watermarkStatus.getKey(), false);

                // if all input channels of the valve are now idle, we need to output an idle stream
                // status from the valve (this also marks the valve as idle)
                if (!InputChannelStatus.hasActiveChannelsKeyed(
                        channelStatuses, watermarkStatus.getKey())) {

                    // now that all input channels are idle and no channels will continue to advance
                    // its
                    // watermark,
                    // we should "flush" all watermarks across all channels; effectively, this means
                    // emitting
                    // the max watermark across all channels as the new watermark. Also, since we
                    // already try to advance
                    // the min watermark as channels individually become IDLE, here we only need to
                    // perform the flush
                    // if the watermark of the last active channel that just became idle is the
                    // current
                    // min watermark.
                    if (channelStatuses[channelIndex].keyedWatermark.getOrDefault(
                                    watermarkStatus.getKey(), Long.MIN_VALUE)
                            == lastOutputWatermarksAndKeys.getOrDefault(
                                    watermarkStatus.getKey(), Long.MIN_VALUE)) {
                        findAndOutputMaxWatermarkAcrossAllChannels(
                                output, watermarkStatus.getKey());
                    }

                    lastOutputWatermarkStatusAndKeys.put(
                            watermarkStatus.getStatus(), WatermarkStatus.IDLE);
                    output.emitWatermarkStatus(
                            lastOutputWatermarkStatusAndKeys.getOrDefault(
                                    watermarkStatus.getKey(), WatermarkStatus.ACTIVE));
                } else if (channelStatuses[channelIndex].keyedWatermark.getOrDefault(
                                watermarkStatus.getKey(), Long.MIN_VALUE)
                        == lastOutputWatermarksAndKeys.getOrDefault(
                                watermarkStatus.getKey(), Long.MIN_VALUE)) {
                    // if the watermark of the channel that just became idle equals the last output
                    // watermark (the previous overall min watermark), we may be able to find a new
                    // min watermark from the remaining aligned channels
                    findAndOutputNewMinWatermarkAcrossAlignedChannels(
                            output, watermarkStatus.getKey());
                }
            } else if (watermarkStatus.isActive()
                    && channelStatuses[channelIndex]
                            .keyedWatermarkStatus
                            .getOrDefault(watermarkStatus.getKey(), WatermarkStatus.ACTIVE)
                            .isIdle()) {
                // handle idle -> active toggle for the input channel
                channelStatuses[channelIndex].keyedWatermarkStatus.put(
                        watermarkStatus.getKey(), WatermarkStatus.ACTIVE);

                // if the last watermark of the input channel, before it was marked idle, is still
                // larger than
                // the overall last output watermark of the valve, then we can set the channel to be
                // aligned already.
                if (channelStatuses[channelIndex].keyedWatermark.getOrDefault(
                                watermarkStatus.getKey(), Long.MIN_VALUE)
                        >= lastOutputWatermarksAndKeys.getOrDefault(
                                watermarkStatus.getKey(), Long.MIN_VALUE)) {
                    channelStatuses[channelIndex].keyedWatermarkAligned.put(
                            watermarkStatus.getKey(), true);
                }

                // if the valve was previously marked to be idle, mark it as active and output an
                // active
                // stream
                // status because at least one of the input channels is now active
                if (lastOutputWatermarkStatusAndKeys
                        .getOrDefault(watermarkStatus.getKey(), WatermarkStatus.ACTIVE)
                        .isIdle()) {
                    lastOutputWatermarkStatusAndKeys.put(
                            watermarkStatus.getKey(), WatermarkStatus.ACTIVE);
                    output.emitWatermarkStatus(
                            lastOutputWatermarkStatusAndKeys.getOrDefault(
                                    watermarkStatus.getKey(), WatermarkStatus.ACTIVE));
                }
            }
        } else {
            // only account for watermark status inputs that will result in a status change for the
            // input
            // channel
            if (watermarkStatus.isIdle()
                    && channelStatuses[channelIndex].watermarkStatus.isActive()) {
                // handle active -> idle toggle for the input channel
                channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.IDLE;

                // the channel is now idle, therefore not aligned
                channelStatuses[channelIndex].isWatermarkAligned = false;

                // if all input channels of the valve are now idle, we need to output an idle stream
                // status from the valve (this also marks the valve as idle)
                if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

                    // now that all input channels are idle and no channels will continue to advance
                    // its
                    // watermark,
                    // we should "flush" all watermarks across all channels; effectively, this means
                    // emitting
                    // the max watermark across all channels as the new watermark. Also, since we
                    // already try to advance
                    // the min watermark as channels individually become IDLE, here we only need to
                    // perform the flush
                    // if the watermark of the last active channel that just became idle is the
                    // current
                    // min watermark.
                    if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                        findAndOutputMaxWatermarkAcrossAllChannels(output, null);
                    }

                    lastOutputWatermarkStatus = WatermarkStatus.IDLE;
                    output.emitWatermarkStatus(lastOutputWatermarkStatus);
                } else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
                    // if the watermark of the channel that just became idle equals the last output
                    // watermark (the previous overall min watermark), we may be able to find a new
                    // min watermark from the remaining aligned channels
                    findAndOutputNewMinWatermarkAcrossAlignedChannels(output, null);
                }
            } else if (watermarkStatus.isActive()
                    && channelStatuses[channelIndex].watermarkStatus.isIdle()) {
                // handle idle -> active toggle for the input channel
                channelStatuses[channelIndex].watermarkStatus = WatermarkStatus.ACTIVE;

                // if the last watermark of the input channel, before it was marked idle, is still
                // larger than
                // the overall last output watermark of the valve, then we can set the channel to be
                // aligned already.
                if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
                    channelStatuses[channelIndex].isWatermarkAligned = true;
                }

                // if the valve was previously marked to be idle, mark it as active and output an
                // active
                // stream
                // status because at least one of the input channels is now active
                if (lastOutputWatermarkStatus.isIdle()) {
                    lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
                    output.emitWatermarkStatus(lastOutputWatermarkStatus);
                }
            }
        }
    }

    private void findAndOutputNewMinWatermarkAcrossAlignedChannels(DataOutput<?> output, Object key)
            throws Exception {
        long newMinWatermark = Long.MAX_VALUE;
        boolean hasAlignedChannels = false;

        // determine new overall watermark by considering only watermark-aligned channels across all
        // channels
        for (InputChannelStatus channelStatus : channelStatuses) {
            // Check if the key is null or not
            if (key != null) { // so it is keyed watermarks
                // We need to loop over all the keys in each channel
                if (channelStatus.keyedWatermarkAligned.getOrDefault(key, true)) {
                    hasAlignedChannelsKeyed.put(key, true);
                    if (newMinWatermarksAndKeys.get(key) == null) {
                        newMinWatermarksAndKeys.put(
                                key,
                                channelStatus.keyedWatermark.getOrDefault(key, Long.MIN_VALUE));
                    } else
                        newMinWatermarksAndKeys.put(
                                key,
                                Math.max(
                                        channelStatus.keyedWatermark.getOrDefault(
                                                key, Long.MIN_VALUE),
                                        newMinWatermarksAndKeys.getOrDefault(key, Long.MAX_VALUE)));
                }
                // we acknowledge and output the new overall watermark if it really is
                // aggregated
                // from some remaining aligned channel, and is also larger than the last output
                // watermark
                if (hasAlignedChannelsKeyed.getOrDefault(key, false)
                        && newMinWatermarksAndKeys.getOrDefault(key, Long.MAX_VALUE)
                                > lastOutputWatermarksAndKeys.getOrDefault(key, Long.MIN_VALUE)) {
                    lastOutputWatermarksAndKeys.put(
                            key, newMinWatermarksAndKeys.getOrDefault(key, Long.MAX_VALUE));
                    output.emitWatermark(
                            new Watermark(
                                    lastOutputWatermarksAndKeys.getOrDefault(key, Long.MIN_VALUE),
                                    key,
                                    true));
                }
            } else {
                if (channelStatus.isWatermarkAligned) {
                    hasAlignedChannels = true;
                    newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
                }
                // we acknowledge and output the new overall watermark if it really is aggregated
                // from some remaining aligned channel, and is also larger than the last output
                // watermark
                if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
                    lastOutputWatermark = newMinWatermark;
                    output.emitWatermark(new Watermark(lastOutputWatermark, Long.MIN_VALUE));
                }
            }
        }
    }

    private void findAndOutputMaxWatermarkAcrossAllChannels(DataOutput<?> output, Object key)
            throws Exception {
        long maxWatermark = Long.MIN_VALUE;

        for (InputChannelStatus channelStatus : channelStatuses) {
            // Check if the key is null or not
            if (key != null) { // so it is keyed watermarks
                // We need to loop over all the keys in each channel
                for (Object wKey : channelStatus.keyedWatermark.keySet()) {
                    maxWatermarksAndKeys.put(
                            wKey,
                            Math.max(
                                    channelStatus.keyedWatermark.getOrDefault(wKey, Long.MIN_VALUE),
                                    maxWatermarksAndKeys.getOrDefault(wKey, Long.MIN_VALUE)));
                    if (maxWatermarksAndKeys.getOrDefault(wKey, Long.MIN_VALUE)
                            > lastOutputWatermarksAndKeys.getOrDefault(wKey, Long.MIN_VALUE)) {
                        lastOutputWatermarksAndKeys.put(
                                wKey, maxWatermarksAndKeys.getOrDefault(wKey, Long.MIN_VALUE));
                        output.emitWatermark(
                                new Watermark(
                                        lastOutputWatermarksAndKeys.getOrDefault(
                                                wKey, Long.MIN_VALUE),
                                        wKey));
                    }
                }
            } else {
                maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
                if (maxWatermark > lastOutputWatermark) {
                    lastOutputWatermark = maxWatermark;
                    output.emitWatermark(new Watermark(lastOutputWatermark, Long.MIN_VALUE));
                }
            }
        }
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the channel is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     */
    @VisibleForTesting
    protected static class InputChannelStatus {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;
        protected HashMap<Object, Long> keyedWatermark;
        protected HashMap<Object, WatermarkStatus> keyedWatermarkStatus;
        protected HashMap<Object, Boolean> keyedWatermarkAligned;

        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         */
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.watermarkStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }

        private static boolean hasActiveChannelsKeyed(
                InputChannelStatus[] channelStatuses, Object key) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.keyedWatermarkStatus.get(key).isActive()) {
                    return true;
                }
            }
            return false;
        }
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }
}
