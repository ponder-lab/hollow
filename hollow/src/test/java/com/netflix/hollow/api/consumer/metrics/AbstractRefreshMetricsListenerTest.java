package com.netflix.hollow.api.consumer.metrics;

import static com.netflix.hollow.core.HollowConstants.VERSION_NONE;

import com.netflix.hollow.api.consumer.HollowConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static com.netflix.hollow.test.AssertShim.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbstractRefreshMetricsListenerTest {

    private final long TEST_VERSION_LOW = 123l;
    private final long TEST_VERSION_HIGH = 456l;

    TestRefreshMetricsListener concreteRefreshMetricsListener;

    class TestRefreshMetricsListener extends AbstractRefreshMetricsListener {
        @Override
        public void refreshEndMetricsReporting(ConsumerRefreshMetrics refreshMetrics) {
            assertNotNull(refreshMetrics);
        }
    }

    @BeforeEach
    public void setup() {
        concreteRefreshMetricsListener = new TestRefreshMetricsListener();
    }

    @Test
    public void testRefreshStartedWithInitialLoad() {
        concreteRefreshMetricsListener.refreshStarted(VERSION_NONE, TEST_VERSION_HIGH);
        ConsumerRefreshMetrics refreshMetrics = concreteRefreshMetricsListener.refreshMetricsBuilder.build();
        assertEquals(true, refreshMetrics.getIsInitialLoad());
        assertNotNull(refreshMetrics.getUpdatePlanDetails());
    }

    @Test
    public void testRefreshStartedWithSubsequentLoad() {
        concreteRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        ConsumerRefreshMetrics refreshMetrics = concreteRefreshMetricsListener.refreshMetricsBuilder.build();
        assertEquals(false, refreshMetrics.getIsInitialLoad());
        assertNotNull(refreshMetrics.getUpdatePlanDetails());
    }

    @Test
    public void testTransitionsPlannedWithSnapshotUpdatePlan() {
        List<HollowConsumer.Blob.BlobType> testTransitionSequence = new ArrayList<HollowConsumer.Blob.BlobType>() {{
            add(HollowConsumer.Blob.BlobType.SNAPSHOT);
            add(HollowConsumer.Blob.BlobType.DELTA);
            add(HollowConsumer.Blob.BlobType.DELTA);
        }};
        concreteRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        concreteRefreshMetricsListener.transitionsPlanned(TEST_VERSION_LOW, TEST_VERSION_HIGH, true, testTransitionSequence);
        ConsumerRefreshMetrics refreshMetrics = concreteRefreshMetricsListener.refreshMetricsBuilder.build();

        assertEquals(HollowConsumer.Blob.BlobType.SNAPSHOT, refreshMetrics.getOverallRefreshType());
        assertEquals(TEST_VERSION_HIGH, refreshMetrics.getUpdatePlanDetails().getDesiredVersion());
        assertEquals(TEST_VERSION_LOW, refreshMetrics.getUpdatePlanDetails().getBeforeVersion());
        assertEquals(testTransitionSequence, refreshMetrics.getUpdatePlanDetails().getTransitionSequence());
    }

    @Test
    public void testTransitionsPlannedWithDeltaUpdatePlan() {
        List<HollowConsumer.Blob.BlobType> testTransitionSequence = new ArrayList<HollowConsumer.Blob.BlobType>() {{
            add(HollowConsumer.Blob.BlobType.DELTA);
            add(HollowConsumer.Blob.BlobType.DELTA);
            add(HollowConsumer.Blob.BlobType.DELTA);
        }};
        concreteRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        concreteRefreshMetricsListener.transitionsPlanned(TEST_VERSION_LOW, TEST_VERSION_HIGH, false, testTransitionSequence);
        ConsumerRefreshMetrics refreshMetrics = concreteRefreshMetricsListener.refreshMetricsBuilder.build();

        assertEquals(HollowConsumer.Blob.BlobType.DELTA, refreshMetrics.getOverallRefreshType());
        assertEquals(TEST_VERSION_HIGH, refreshMetrics.getUpdatePlanDetails().getDesiredVersion());
        assertEquals(TEST_VERSION_LOW, refreshMetrics.getUpdatePlanDetails().getBeforeVersion());
        assertEquals(testTransitionSequence, refreshMetrics.getUpdatePlanDetails().getTransitionSequence());
    }

    @Test
    public void testTransitionsPlannedWithReverseDeltaUpdatePlan() {
        List<HollowConsumer.Blob.BlobType> testTransitionSequence = new ArrayList<HollowConsumer.Blob.BlobType>() {{
            add(HollowConsumer.Blob.BlobType.REVERSE_DELTA);
            add(HollowConsumer.Blob.BlobType.REVERSE_DELTA);
            add(HollowConsumer.Blob.BlobType.REVERSE_DELTA);
        }};
        concreteRefreshMetricsListener.refreshStarted(TEST_VERSION_HIGH, TEST_VERSION_LOW);
        concreteRefreshMetricsListener.transitionsPlanned(TEST_VERSION_HIGH, TEST_VERSION_LOW, false, testTransitionSequence);
        ConsumerRefreshMetrics refreshMetrics = concreteRefreshMetricsListener.refreshMetricsBuilder.build();

        assertEquals(HollowConsumer.Blob.BlobType.REVERSE_DELTA, refreshMetrics.getOverallRefreshType());
        assertEquals(TEST_VERSION_LOW, refreshMetrics.getUpdatePlanDetails().getDesiredVersion());
        assertEquals(TEST_VERSION_HIGH, refreshMetrics.getUpdatePlanDetails().getBeforeVersion());
        assertEquals(testTransitionSequence, refreshMetrics.getUpdatePlanDetails().getTransitionSequence());
    }

    @Test
    public void testRefreshSuccess() {
        class SuccessTestRefreshMetricsListener extends AbstractRefreshMetricsListener {
            @Override
            public void refreshEndMetricsReporting(ConsumerRefreshMetrics refreshMetrics) {
                assertEquals(0l, refreshMetrics.getConsecutiveFailures());
                assertEquals(true, refreshMetrics.getIsRefreshSuccess());
                assertEquals(0l, refreshMetrics.getRefreshSuccessAgeMillisOptional().getAsLong());
                assertNotEquals(0l, refreshMetrics.getRefreshEndTimeNano());
            }
        }
        SuccessTestRefreshMetricsListener successTestRefreshMetricsListener = new SuccessTestRefreshMetricsListener();
        successTestRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        successTestRefreshMetricsListener.refreshSuccessful(TEST_VERSION_LOW, TEST_VERSION_HIGH, TEST_VERSION_HIGH);
    }

    @Test
    public void testRefreshFailure() {
        class FailureTestRefreshMetricsListener extends AbstractRefreshMetricsListener {
            @Override
            public void refreshEndMetricsReporting(ConsumerRefreshMetrics refreshMetrics) {
                assertNotEquals(0l, refreshMetrics.getConsecutiveFailures());
                assertEquals(false, refreshMetrics.getIsRefreshSuccess());
                assertNotEquals(Optional.empty(), refreshMetrics.getRefreshSuccessAgeMillisOptional());
                assertNotEquals(0l, refreshMetrics.getRefreshEndTimeNano());
            }
        }
        FailureTestRefreshMetricsListener successTestRefreshMetricsListener = new FailureTestRefreshMetricsListener();
        successTestRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        successTestRefreshMetricsListener.refreshFailed(TEST_VERSION_LOW, TEST_VERSION_HIGH, TEST_VERSION_HIGH, null);

    }

    @Test
    public void testMetricsWhenMultiTransitionRefreshSucceeds() {
        class SuccessTestRefreshMetricsListener extends AbstractRefreshMetricsListener {
            @Override
            public void refreshEndMetricsReporting(ConsumerRefreshMetrics refreshMetrics) {
                assertEquals(3, refreshMetrics.getUpdatePlanDetails().getNumSuccessfulTransitions());
            }
        }
        List<HollowConsumer.Blob.BlobType> testTransitionSequence = new ArrayList<HollowConsumer.Blob.BlobType>() {{
            add(HollowConsumer.Blob.BlobType.SNAPSHOT);
            add(HollowConsumer.Blob.BlobType.DELTA);
            add(HollowConsumer.Blob.BlobType.DELTA);
        }};

        SuccessTestRefreshMetricsListener successTestRefreshMetricsListener = new SuccessTestRefreshMetricsListener();
        successTestRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        successTestRefreshMetricsListener.transitionsPlanned(TEST_VERSION_LOW, TEST_VERSION_HIGH, true, testTransitionSequence);
        successTestRefreshMetricsListener.blobLoaded(null);
        successTestRefreshMetricsListener.blobLoaded(null);
        successTestRefreshMetricsListener.blobLoaded(null);
        successTestRefreshMetricsListener.refreshSuccessful(TEST_VERSION_LOW, TEST_VERSION_HIGH, TEST_VERSION_HIGH);
    }

    @Test
    public void testMetricsWhenMultiTransitionRefreshFails() {
        class FailureTestRefreshMetricsListener extends AbstractRefreshMetricsListener {
            @Override
            public void refreshEndMetricsReporting(ConsumerRefreshMetrics refreshMetrics) {
                assertEquals(1, refreshMetrics.getUpdatePlanDetails().getNumSuccessfulTransitions());
            }
        }
        List<HollowConsumer.Blob.BlobType> testTransitionSequence = new ArrayList<HollowConsumer.Blob.BlobType>() {{
            add(HollowConsumer.Blob.BlobType.SNAPSHOT);
            add(HollowConsumer.Blob.BlobType.DELTA);
            add(HollowConsumer.Blob.BlobType.DELTA);
        }};

        FailureTestRefreshMetricsListener failureTestRefreshMetricsListener = new FailureTestRefreshMetricsListener();
        failureTestRefreshMetricsListener.refreshStarted(TEST_VERSION_LOW, TEST_VERSION_HIGH);
        failureTestRefreshMetricsListener.transitionsPlanned(TEST_VERSION_LOW, TEST_VERSION_HIGH, true, testTransitionSequence);
        failureTestRefreshMetricsListener.blobLoaded(null);
        failureTestRefreshMetricsListener.refreshFailed(TEST_VERSION_LOW, TEST_VERSION_HIGH, TEST_VERSION_HIGH, null);

    }
}
