package com.example.fanin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Sink connector that writes every record of the subscribed topics to one target topic.
 *
 * <p>The subscription itself ({@code topics} or {@code topics.regex}) is handled by the Connect framework.
 */
public class FanInSinkConnector extends SinkConnector {

    /** Internal per-task property, used to give every task's producer a distinct client.id. */
    static final String TASK_INDEX = "fanin.task.index";

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        new FanInSinkConfig(props); // fail fast on a bad config
        this.configProps = new HashMap<>(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FanInSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put(TASK_INDEX, Integer.toString(i));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        // nothing to release
    }

    @Override
    public ConfigDef config() {
        return FanInSinkConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        Map<String, ConfigValue> values = new HashMap<>();
        for (ConfigValue value : config.configValues()) {
            values.put(value.name(), value);
        }

        String topics = connectorConfigs.get(FanInSinkConfig.TOPICS);
        String regex = connectorConfigs.get(FanInSinkConfig.TOPICS_REGEX);
        boolean hasTopics = topics != null && !topics.isBlank();
        boolean hasRegex = regex != null && !regex.isBlank();

        if (hasTopics == hasRegex) {
            String message = "Exactly one of '" + FanInSinkConfig.TOPICS + "' or '" + FanInSinkConfig.TOPICS_REGEX
                + "' must be set, " + (hasTopics ? "but both are." : "but neither is.");
            addError(config, values, FanInSinkConfig.TOPICS, message);
            addError(config, values, FanInSinkConfig.TOPICS_REGEX, message);
        }

        FanInSinkConfig.targetSubscriptionError(connectorConfigs).ifPresent(message -> {
            String key = message.startsWith("topics contains") ? FanInSinkConfig.TOPICS
                : FanInSinkConfig.TOPICS_REGEX;
            addError(config, values, key, message);
        });
        return config;
    }

    private static void addError(Config config, Map<String, ConfigValue> values, String name, String message) {
        ConfigValue value = values.get(name);
        if (value == null) {
            value = new ConfigValue(name);
            values.put(name, value);
            config.configValues().add(value);
        }
        value.addErrorMessage(message);
    }

    @Override
    public String version() {
        return Version.get();
    }
}
