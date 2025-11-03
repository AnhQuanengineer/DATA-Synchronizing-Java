package org.datapipeline.Config;

public class KafkaConfig extends ValidateConfig {
    private final String bootstrapServers;
    private final String topic;
    private final String group_id;

    public KafkaConfig(String bootstrapServers, String topic, String group_id) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.group_id = group_id;
        super.validate();
    }
    public String getBootstrapServers() {
        return bootstrapServers;
    }
    public String getTopic() {
        return topic;
    }
    public String getGroup_id() {
        return group_id;
    }
}
