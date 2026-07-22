package com.aliyun.dts.subscribe.clients.metastore;

import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class MetaStoreJsonCompatibilityTest {

    private static final String GROUP_ID = "subscription-group";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("dts-topic", 0);
    private static final String LEGACY_CHECKPOINT_JSON = "{\"groupID\":\"subscription-group\","
            + "\"streamCheckpoint\":[{\"topic\":\"dts-topic\",\"partition\":0,\"offset\":42,"
            + "\"timestamp\":1720000000,\"info\":\"legacy-checkpoint\"}]}";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void abstractUserMetaStoreReadsLegacyCheckpointAndRoundTripsWithFastjson2() throws Exception {
        InMemoryMetaStore metaStore = new InMemoryMetaStore();
        metaStore.storedData = LEGACY_CHECKPOINT_JSON;

        assertCheckpoint(metaStore.deserializeFrom(TOPIC_PARTITION, GROUP_ID));

        Checkpoint checkpoint = new Checkpoint(TOPIC_PARTITION, 1720000000L, 42L, "legacy-checkpoint");
        metaStore.serializeTo(TOPIC_PARTITION, GROUP_ID, checkpoint).get();

        InMemoryMetaStore reloadedMetaStore = new InMemoryMetaStore();
        reloadedMetaStore.storedData = metaStore.storedData;
        assertCheckpoint(reloadedMetaStore.deserializeFrom(TOPIC_PARTITION, GROUP_ID));
    }

    @Test
    public void localFileMetaStoreReadsLegacyCheckpoint() throws Exception {
        File checkpointFile = temporaryFolder.newFile("checkpoint.json");
        Files.write(checkpointFile.toPath(), LEGACY_CHECKPOINT_JSON.getBytes(StandardCharsets.UTF_8));

        LocalFileMetaStore metaStore = new LocalFileMetaStore(checkpointFile.getAbsolutePath());

        assertCheckpoint(metaStore.deserializeFrom(TOPIC_PARTITION, GROUP_ID));
    }

    private static void assertCheckpoint(Checkpoint checkpoint) {
        Assert.assertNotNull(checkpoint);
        Assert.assertEquals(TOPIC_PARTITION, checkpoint.getTopicPartition());
        Assert.assertEquals(1720000000L, checkpoint.getTimeStamp());
        Assert.assertEquals(42L, checkpoint.getOffset());
        Assert.assertEquals("legacy-checkpoint", checkpoint.getInfo());
    }

    private static class InMemoryMetaStore extends AbstractUserMetaStore {
        private String storedData;

        @Override
        protected void saveData(String groupID, String toStoreJson) {
            storedData = toStoreJson;
        }

        @Override
        protected String getData(String groupID) {
            return storedData;
        }
    }
}
