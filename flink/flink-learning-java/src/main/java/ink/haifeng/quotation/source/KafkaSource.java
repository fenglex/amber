package ink.haifeng.quotation.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/18 16:07:50
 */
public class KafkaSource implements CustomizedSource {

    private String bootstrap;
    private String topic;
    private String groupId;

    public KafkaSource(String bootstrap, String topic, String groupId) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.groupId = groupId;
    }


    @Override
    public Source source() {
        return org.apache.flink.connector.kafka.source.KafkaSource.<String>builder()
        .setBootstrapServers(bootstrap)
        .setTopics(topic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
    }
}
