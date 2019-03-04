/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.api.consumer;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.ReadState;
import com.netflix.hollow.api.producer.HollowProducer.VersionMinter;
import com.netflix.hollow.api.producer.fs.HollowInMemoryBlobStager;
import com.netflix.hollow.api.producer.validation.ValidationResult;
import com.netflix.hollow.api.producer.validation.ValidationResultType;
import com.netflix.hollow.api.producer.validation.ValidationStatus;
import com.netflix.hollow.api.producer.validation.ValidationStatusException;
import com.netflix.hollow.api.producer.validation.ValidationStatusListener;
import com.netflix.hollow.api.producer.validation.ValidatorListener;
import com.netflix.hollow.core.read.engine.object.HollowObjectTypeReadState;
import com.netflix.hollow.tools.compact.HollowCompactor.CompactionConfig;
import java.time.Duration;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HollowProducerConsumerTests {

    private InMemoryBlobStore blobStore;
    private InMemoryAnnouncement announcement;

    @BeforeEach
    public void setUp() {
        blobStore = new InMemoryBlobStore();
        announcement = new InMemoryAnnouncement();
    }

    @Test
    public void publishAndLoadASnapshot() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        /// Showing verbose version of `runCycle(producer, 1);`
        long version = producer.runCycle(state -> state.add(1));

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefreshTo(version);

        assertEquals(version, consumer.getCurrentVersionId());
    }

    @Test
    public void initializationTraversesDeltasToGetUpToDate() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withNumStatesBetweenSnapshots(2) /// do not produce snapshots for v2 or v3
                .build();

        long v1 = runCycle(producer, 1);
        long v2 = runCycle(producer, 2);
        long v3 = runCycle(producer, 3);

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefreshTo(v3);

        assertEquals(v3, consumer.getCurrentVersionId());

        assertEquals(v1, blobStore.retrieveSnapshotBlob(v3).getToVersion());
        assertEquals(v2, blobStore.retrieveDeltaBlob(v1).getToVersion());
        assertEquals(v3, blobStore.retrieveDeltaBlob(v2).getToVersion());
    }

    @Test
    public void consumerAutomaticallyUpdatesBasedOnAnnouncement() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withAnnouncer(announcement)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        long v1 = runCycle(producer, 1);

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore)
                .withAnnouncementWatcher(announcement)
                .build();
        consumer.triggerRefresh();

        assertEquals(v1, consumer.getCurrentVersionId());

        long v2 = runCycle(producer, 2);

        assertEquals(v2, consumer.getCurrentVersionId());
    }

    @Test
    public void consumerFollowsReverseDeltas() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withNumStatesBetweenSnapshots(2) /// do not produce snapshot for v2 or v3
                .build();

        long v1 = runCycle(producer, 1);
        runCycle(producer, 2);
        long v3 = runCycle(producer, 3);

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefreshTo(v3);

        assertEquals(v3, consumer.getCurrentVersionId());

        blobStore.removeSnapshot(
                v1); // <-- not necessary to cause following of reverse deltas -- just asserting that's what happened.
        consumer.triggerRefreshTo(v1);

        assertEquals(v1, consumer.getCurrentVersionId());
    }

    @Test
    public void consumerRespondsToPinnedAnnouncement() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withAnnouncer(announcement)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withNumStatesBetweenSnapshots(2) /// do not produce snapshot for v2 or v3
                .build();

        long v1 = runCycle(producer, 1);
        runCycle(producer, 2);
        long v3 = runCycle(producer, 3);


        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore)
                .withAnnouncementWatcher(announcement)
                .build();
        consumer.triggerRefresh();

        assertEquals(v3, consumer.getCurrentVersionId());

        announcement.pin(v1);

        assertEquals(v1, consumer.getCurrentVersionId());

        /// another cycle occurs while we're pinned
        long v4 = runCycle(producer, 4);

        announcement.unpin();

        assertEquals(v4, consumer.getCurrentVersionId());
    }

    @Test
    public void consumerFindsLatestPublishedVersionWithoutAnnouncementWatcher() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withAnnouncer(announcement)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        long v1 = runCycle(producer, 1);

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();

        consumer.triggerRefresh();
        assertEquals(v1, consumer.getCurrentVersionId());

        consumer.triggerRefresh();
        assertEquals(v1, consumer.getCurrentVersionId());

        long v2 = runCycle(producer, 2);

        consumer.triggerRefresh();
        assertEquals(v2, consumer.getCurrentVersionId());
    }

    @Test
    public void producerRestoresAndProducesDelta() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        long v1 = runCycle(producer, 1);

        HollowProducer redeployedProducer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        redeployedProducer.initializeDataModel(Integer.class);
        redeployedProducer.restore(v1, blobStore);

        long v2 = runCycle(producer, 2);

        assertNotNull(blobStore.retrieveDeltaBlob(v1));
        assertEquals(v2, blobStore.retrieveDeltaBlob(v1).getToVersion());
    }

    @Test
    public void producerUsesCustomSnapshotPublisherExecutor() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withSnapshotPublishExecutor(command -> {
                    /// do not publish snapshots!
                })
                .build();

        long v1 = runCycle(producer, 1);
        long v2 = runCycle(producer, 2);
        long v3 = runCycle(producer, 3);
        long v4 = runCycle(producer, 4);

        /// first cycle always publishes in-band -- does not use the Executor, so we expect a snapshot for v1.
        assertEquals(v1, blobStore.retrieveSnapshotBlob(v1).getToVersion());
        assertEquals(v1, blobStore.retrieveSnapshotBlob(v2).getToVersion());
        assertEquals(v1, blobStore.retrieveSnapshotBlob(v3).getToVersion());
        assertEquals(v1, blobStore.retrieveSnapshotBlob(v4).getToVersion());
    }

    @Test
    public void producerUsesCustomVersionMinter() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withVersionMinter(new VersionMinter() {
                    long counter = 0;

                    public long mint() {
                        return ++counter;
                    }
                })
                .build();

        long v1 = runCycle(producer, 1);
        long v2 = runCycle(producer, 2);
        long v3 = runCycle(producer, 3);

        assertEquals(1, v1);
        assertEquals(2, v2);
        assertEquals(3, v3);
    }

    @Test
    public void producerValidatesWithFailure() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withListener(new ValidatorListener() {
                    @Override public String getName() {
                        return "Test validator";
                    }

                    @Override public ValidationResult onValidate(ReadState readState) {
                        return ValidationResult.from(this).failed("Expected to fail!");
                    }
                })
                .withListener(new ValidationStatusListener() {
                    boolean isStartCalled;

                    @Override public void onValidationStatusStart(long version) {
                        isStartCalled = true;
                    }

                    @Override public void onValidationStatusComplete(
                            ValidationStatus status, long version, Duration elapsed) {
                        assertTrue(isStartCalled);
                        assertTrue(status.failed());
                        assertEquals(1, status.getResults().size());

                        ValidationResult r = status.getResults().get(0);
                        assertEquals("Test validator", r.getName());
                        assertEquals("Expected to fail!", r.getMessage());
                        assertEquals(ValidationResultType.FAILED, r.getResultType());
                    }
                })
                .build();

        try {
            runCycle(producer, 1);
            fail();
        } catch (ValidationStatusException expected) {
            ValidationStatus status = expected.getValidationStatus();
            assertTrue(status.failed());
            assertEquals(1, status.getResults().size());

            ValidationResult r = status.getResults().get(0);
            assertEquals("Test validator", r.getName());
            assertEquals("Expected to fail!", r.getMessage());
            assertEquals(ValidationResultType.FAILED, r.getResultType());
        }
    }

    @Test
    public void producerValidatesWithError() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withListener(new ValidatorListener() {
                    @Override public String getName() {
                        return "Test validator";
                    }

                    @Override public ValidationResult onValidate(ReadState readState) {
                        throw new RuntimeException("Expected to fail!");
                    }
                })
                .withListener(new ValidationStatusListener() {
                    boolean isStartCalled;

                    @Override public void onValidationStatusStart(long version) {
                        isStartCalled = true;
                    }

                    @Override public void onValidationStatusComplete(
                            ValidationStatus status, long version, Duration elapsed) {
                        assertTrue(isStartCalled);
                        assertTrue(status.failed());
                        assertEquals(1, status.getResults().size());

                        ValidationResult r = status.getResults().get(0);
                        assertEquals("Test validator", r.getName());
                        assertEquals("Expected to fail!", r.getMessage());
                        assertEquals(ValidationResultType.ERROR, r.getResultType());
                    }
                })
                .build();

        try {
            runCycle(producer, 1);
            fail();
        } catch (ValidationStatusException expected) {
            ValidationStatus status = expected.getValidationStatus();
            assertTrue(status.failed());
            assertEquals(1, status.getResults().size());

            ValidationResult r = status.getResults().get(0);
            assertEquals("Test validator", r.getName());
            assertEquals("Expected to fail!", r.getMessage());
            assertEquals(ValidationResultType.ERROR, r.getResultType());
        }
    }

    @Test
    public void producerCanContinueAfterValidationFailureNew() {
        AtomicInteger counter = new AtomicInteger();
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .withListener(new ValidatorListener() {
                    @Override
                    public String getName() {
                        return "Test validator";
                    }

                    @Override
                    public ValidationResult onValidate(ReadState readState) {
                        if (counter.incrementAndGet() == 2) {
                            return ValidationResult.from(this).failed("Expected to fail!");
                        } else {
                            return ValidationResult.from(this).passed("Pass");
                        }
                    }
                })
                .withListener(new ValidationStatusListener() {
                    @Override public void onValidationStatusStart(long version) {
                    }

                    @Override public void onValidationStatusComplete(
                            ValidationStatus status, long version, Duration elapsed) {
                        if (counter.get() == 2) {
                            assertTrue(status.failed());
                            assertEquals(1, status.getResults().size());

                            ValidationResult r = status.getResults().get(0);
                            assertEquals("Test validator", r.getName());
                            assertEquals("Expected to fail!", r.getMessage());
                            assertEquals(ValidationResultType.FAILED, r.getResultType());
                        } else {
                            assertTrue(status.passed());
                            assertEquals(1, status.getResults().size());

                            ValidationResult r = status.getResults().get(0);
                            assertEquals("Test validator", r.getName());
                            assertEquals("Pass", r.getMessage());
                            assertEquals(ValidationResultType.PASSED, r.getResultType());
                        }
                    }
                })
                .build();

        runCycle(producer, 1);

        try {
            runCycle(producer, 2);
            fail();
        } catch (ValidationStatusException expected) {
            ValidationStatus status = expected.getValidationStatus();
            assertTrue(status.failed());
            assertEquals(1, status.getResults().size());

            ValidationResult r = status.getResults().get(0);
            assertEquals("Test validator", r.getName());
            assertEquals("Expected to fail!", r.getMessage());
            assertEquals(ValidationResultType.FAILED, r.getResultType());
        }

        runCycle(producer, 3);
    }

    @Test
    public void producerCompacts() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        producer.runCycle(state -> {
            for (int i = 0; i < 10000; i++) {
                state.add(i);
            }
        });

        long v2 = producer.runCycle(state -> {
            for (int i = 10000; i < 20000; i++) {
                state.add(i);
            }
        });

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefreshTo(v2);

        /// assert that a compaction is now necessary
        long popOrdinalsLength = consumer.getStateEngine().getTypeState("Integer").getPopulatedOrdinals().length();
        assertEquals(20000, popOrdinalsLength);

        /// run a compaction cycle
        long v3 = producer.runCompactionCycle(new CompactionConfig(0, 20));

        /// assert that a compaction actually happened
        consumer.triggerRefreshTo(v3);
        popOrdinalsLength = consumer.getStateEngine().getTypeState("Integer").getPopulatedOrdinals().length();
        assertEquals(10000, popOrdinalsLength);

        BitSet foundValues = new BitSet(20000);
        for (int i = 0; i < popOrdinalsLength; i++) {
            foundValues.set(((HollowObjectTypeReadState) consumer.getStateEngine().getTypeState("Integer"))
                    .readInt(i, 0));
        }

        for (int i = 10000; i < 20000; i++) {
            assertTrue(foundValues.get(i));
        }
    }

    private long runCycle(HollowProducer producer, final int cycleNumber) {
        return producer.runCycle(state -> state.add(cycleNumber));
    }
}
