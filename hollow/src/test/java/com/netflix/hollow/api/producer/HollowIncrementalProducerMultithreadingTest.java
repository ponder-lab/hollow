package com.netflix.hollow.api.producer;

import static com.netflix.hollow.test.AssertShim.assertFalse;
import static com.netflix.hollow.test.AssertShim.assertTrue;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.InMemoryBlobStore;
import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.api.producer.fs.HollowInMemoryBlobStager;
import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("large")
public class HollowIncrementalProducerMultithreadingTest {

    private static final int ELEMENTS = 20000;
    private static final double THREADS = 4.0d;
    private static final int ITERATIONS = 10;

    private InMemoryBlobStore blobStore;

    @BeforeEach
    public void setUp() {
        blobStore = new InMemoryBlobStore();
    }

    @Test
    public void updateAndPublishUsingMultithreading() {
        // run within a loop to increase the likelihood of a race condition to occur
        for (int iterationCounter = 0; iterationCounter < ITERATIONS; ++iterationCounter) {
            HollowProducer producer = createInMemoryProducer();

            /// initialize the data -- classic producer creates the first state in the delta chain.
            initializeData(producer);

            /// now we'll be incrementally updating the state by mutating individual records
            HollowIncrementalProducer incrementalProducer = new HollowIncrementalProducer(producer, THREADS);

            int[] notModifiedElementIds = IntStream.range(0, ELEMENTS / 2).toArray();
            int[] modifiedElementIds = IntStream.range(ELEMENTS / 2, ELEMENTS).toArray();

            incrementalProducer.addOrModify(Arrays.stream(modifiedElementIds).mapToObj(i -> new SimpleType(i, i + 1)).collect(Collectors.toList()));

            /// .runCycle() flushes the changes to a new data state.
            long versionAfterUpdate = incrementalProducer.runCycle();

            /// now we read the changes and assert
            HollowPrimaryKeyIndex idx = createPrimaryKeyIndex(versionAfterUpdate);
            assertFalse(idx.containsDuplicates());
            assertTrue(Arrays.stream(notModifiedElementIds)
                                      .boxed()
                                      .map(elementId -> getHollowObject(idx, elementId))
                                      .allMatch(obj -> obj.getInt("value") == obj.getInt("id")));
            assertTrue(Arrays.stream(modifiedElementIds)
                                      .boxed()
                                      .map(elementId -> getHollowObject(idx, elementId))
                                      .allMatch(obj -> obj.getInt("value") != obj.getInt("id")));
        }
    }

    @Test
    public void removeAndPublishUsingMultithreading() {
        // run within a loop to increase the likelihood of a race condition to occur
        for (int iterationCounter = 0; iterationCounter < ITERATIONS; ++iterationCounter) {
            HollowProducer producer = createInMemoryProducer();

            /// initialize the data -- classic producer creates the first state in the delta chain.
            initializeData(producer);

            /// now we'll be incrementally updating the state by mutating individual records
            HollowIncrementalProducer incrementalProducer = new HollowIncrementalProducer(producer, THREADS);

            int[] notModifiedElementIds = IntStream.range(0, ELEMENTS / 2).toArray();
            int[] deletedElementIds = IntStream.range(ELEMENTS / 2, ELEMENTS).toArray();

            incrementalProducer.delete(Arrays.stream(deletedElementIds).mapToObj(i -> new SimpleType(i, i)).collect(Collectors.toList()));

            /// .runCycle() flushes the changes to a new data state.
            long versionAfterDelete = incrementalProducer.runCycle();

            /// now we read the changes and assert
            HollowPrimaryKeyIndex idx = createPrimaryKeyIndex(versionAfterDelete);
            assertTrue(Arrays.stream(notModifiedElementIds)
                                      .boxed()
                                      .map(elementId -> getHollowObject(idx, elementId))
                                      .allMatch(obj -> obj.getInt("value") == obj.getInt("id")));
            assertTrue(Arrays.stream(deletedElementIds)
                                      .boxed()
                                      .map(elementId -> getOrdinal(idx, elementId))
                                      .allMatch(ordinal -> ordinal == -1));
        }
    }

    private HollowProducer createInMemoryProducer() {
        return HollowProducer.withPublisher(blobStore).withBlobStager(new HollowInMemoryBlobStager()).build();
    }

    private void initializeData(HollowProducer producer) {
        producer.runCycle(state -> {
            for (int i = 0; i < ELEMENTS; ++i) {
                state.add(new SimpleType(i, i));
            }
        });
    }

    private HollowPrimaryKeyIndex createPrimaryKeyIndex(long version) {
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore).build();
        consumer.triggerRefreshTo(version);

        return new HollowPrimaryKeyIndex(consumer.getStateEngine(), "SimpleType", "id");
    }

    private GenericHollowObject getHollowObject(HollowPrimaryKeyIndex idx, Object... keys) {
        int ordinal = getOrdinal(idx, keys);
        return new GenericHollowObject(idx.getTypeState(), ordinal);
    }

    private int getOrdinal(HollowPrimaryKeyIndex idx, Object... keys) {
        return idx.getMatchingOrdinal(keys);
    }

    @HollowPrimaryKey(fields = "id")
    private static class SimpleType {

        int id;
        int value;

        SimpleType(int id, int value) {
            this.id = id;
            this.value = value;
        }
    }
}
