package com.netflix.hollow.api.metrics;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.InMemoryBlobStore;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowInMemoryBlobStager;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HollowMetricsCollectorTests {

    private InMemoryBlobStore blobStore;

    @BeforeEach
    public void setUp() {
        blobStore = new InMemoryBlobStore();
    }

    @Test
    public void testNullMetricsCollector() {
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                                                .withBlobStager(new HollowInMemoryBlobStager())
                                                .build();

        long version = producer.runCycle(new HollowProducer.Populator() {
            public void populate(HollowProducer.WriteState state) throws Exception {
                state.add(Integer.valueOf(1));
            }
        });

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore)
                                                .withMetricsCollector(null)
                                                .build();
        consumer.triggerRefreshTo(version);
    }

    @Test
    public void metricsCollectorWhenPublishingSnapshot() {
        FakePublisherHollowMetricsCollector metricsCollector = new FakePublisherHollowMetricsCollector();
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withMetricsCollector(metricsCollector)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        producer.runCycle(new HollowProducer.Populator() {
            public void populate(HollowProducer.WriteState state) throws Exception {
                state.add(Integer.valueOf(1));
            }
        });

        HollowProducerMetrics hollowProducerMetrics = metricsCollector.getMetrics();
        assertEquals(metricsCollector.getMetricsCollected(), true);
        assertEquals(hollowProducerMetrics.getCyclesSucceeded(), 1);
        assertEquals(hollowProducerMetrics.getCyclesCompleted(), 1);
        assertEquals(hollowProducerMetrics.getTotalPopulatedOrdinals(), 1);
        assertEquals(hollowProducerMetrics.getSnapshotsCompleted(), 1);
    }

    @Test
    public void metricsCollectorWhenLoadingSnapshot() {
        FakeConsumerHollowMetricsCollector metricsCollector = new FakeConsumerHollowMetricsCollector();
        HollowProducer producer = HollowProducer.withPublisher(blobStore)
                .withBlobStager(new HollowInMemoryBlobStager())
                .build();

        long version = producer.runCycle(new HollowProducer.Populator() {
            public void populate(HollowProducer.WriteState state) throws Exception {
                state.add(Integer.valueOf(1));
            }
        });

        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobStore)
                .withMetricsCollector(metricsCollector)
                .build();
        consumer.triggerRefreshTo(version);

        HollowConsumerMetrics hollowConsumerMetrics = metricsCollector.getMetrics();
        assertEquals(metricsCollector.getMetricsCollected(), true);
        assertEquals(version, consumer.getCurrentVersionId());
        assertEquals(hollowConsumerMetrics.getCurrentVersion(), consumer.getCurrentVersionId());
        assertEquals(hollowConsumerMetrics.getRefreshSucceded(), 1);
        assertEquals(hollowConsumerMetrics.getTotalPopulatedOrdinals(), 1);
    }

    private static class FakePublisherHollowMetricsCollector extends HollowMetricsCollector<HollowProducerMetrics> {

        private HollowProducerMetrics metrics;
        private boolean metricsCollected;

        @Override
        public void collect(HollowProducerMetrics metrics) {
            this.metrics = metrics;
            this.metricsCollected = true;
        }

        public HollowProducerMetrics getMetrics() {
            return this.metrics;
        }

        public boolean getMetricsCollected() {
            return this.metricsCollected;
        }
    }

    private static class FakeConsumerHollowMetricsCollector extends HollowMetricsCollector<HollowConsumerMetrics> {

        private HollowConsumerMetrics metrics;
        private boolean metricsCollected;

        @Override
        public void collect(HollowConsumerMetrics metrics) {
            this.metrics = metrics;
            this.metricsCollected = true;
        }

        public HollowConsumerMetrics getMetrics() {
            return this.metrics;
        }

        public boolean getMetricsCollected() {
            return this.metricsCollected;
        }
    }
}
