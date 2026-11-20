package com.example.fanin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.junit.jupiter.api.Test;

class FanInSinkConnectorTest {

    private static Map<String, String> base() {
        Map<String, String> props = new HashMap<>();
        props.put("fanin.target.topic", "fanin-out");
        props.put("fanin.target.bootstrap.servers", "localhost:9092");
        props.put("fanin.target.api.key", "key");
        props.put("fanin.target.api.secret", "secret");
        props.put("topics.regex", "^acme\\.app\\..+");
        return props;
    }

    private static List<String> errors(Config config, String name) {
        return config.configValues().stream()
            .filter(v -> v.name().equals(name))
            .flatMap(v -> v.errorMessages().stream())
            .toList();
    }

    private static long totalErrors(Config config) {
        return config.configValues().stream().mapToLong(v -> v.errorMessages().size()).sum();
    }

    @Test
    void validConfigHasNoErrors() {
        assertEquals(0, totalErrors(new FanInSinkConnector().validate(base())));
    }

    @Test
    void validWithTopicsListInsteadOfRegex() {
        Map<String, String> props = base();
        props.remove("topics.regex");
        props.put("topics", "a,b");
        assertEquals(0, totalErrors(new FanInSinkConnector().validate(props)));
    }

    @Test
    void missingTargetTopicIsRejected() {
        Map<String, String> props = base();
        props.remove("fanin.target.topic");
        Config config = new FanInSinkConnector().validate(props);
        assertFalse(errors(config, "fanin.target.topic").isEmpty());
    }

    @Test
    void missingBootstrapKeyAndSecretAreRejected() {
        Map<String, String> props = base();
        props.remove("fanin.target.bootstrap.servers");
        props.remove("fanin.target.api.key");
        props.remove("fanin.target.api.secret");
        Config config = new FanInSinkConnector().validate(props);
        assertFalse(errors(config, "fanin.target.bootstrap.servers").isEmpty());
        assertFalse(errors(config, "fanin.target.api.key").isEmpty());
        assertFalse(errors(config, "fanin.target.api.secret").isEmpty());
    }

    @Test
    void neitherTopicsNorRegexIsRejected() {
        Map<String, String> props = base();
        props.remove("topics.regex");
        Config config = new FanInSinkConnector().validate(props);
        assertTrue(errors(config, "topics.regex").get(0).contains("neither"));
        assertFalse(errors(config, "topics").isEmpty());
    }

    @Test
    void bothTopicsAndRegexIsRejected() {
        Map<String, String> props = base();
        props.put("topics", "a");
        Config config = new FanInSinkConnector().validate(props);
        assertTrue(errors(config, "topics.regex").get(0).contains("both"));
    }

    @Test
    void regexMatchingTargetTopicIsRejected() {
        Map<String, String> props = base();
        props.put("fanin.target.topic", "acme.app.fanin");
        Config config = new FanInSinkConnector().validate(props);
        List<String> errs = errors(config, "topics.regex");
        assertEquals(1, errs.size());
        assertTrue(errs.get(0).contains("matches the target topic"));
    }

    @Test
    void regexUsesFullMatchSemantics() {
        // The framework subscribes with a full match, so a prefix-only overlap is not a problem.
        Map<String, String> props = base();
        props.put("topics.regex", "^out$");
        props.put("fanin.target.topic", "out-fanin");
        assertEquals(0, totalErrors(new FanInSinkConnector().validate(props)));
    }

    @Test
    void topicsListContainingTargetIsRejected() {
        Map<String, String> props = base();
        props.remove("topics.regex");
        props.put("topics", "a, fanin-out");
        Config config = new FanInSinkConnector().validate(props);
        assertFalse(errors(config, "topics").isEmpty());
    }

    @Test
    void invalidRegexIsRejected() {
        Map<String, String> props = base();
        props.put("topics.regex", "([unclosed");
        Config config = new FanInSinkConnector().validate(props);
        assertFalse(errors(config, "topics.regex").isEmpty());
    }

    @Test
    void invalidEnumValueIsRejected() {
        Map<String, String> props = base();
        props.put("fanin.output.mode", "XML");
        Config config = new FanInSinkConnector().validate(props);
        assertFalse(errors(config, "fanin.output.mode").isEmpty());
    }

    @Test
    void configDefDoesNotDefineFrameworkProperties() {
        assertFalse(new FanInSinkConnector().config().names().contains("topics"));
        assertFalse(new FanInSinkConnector().config().names().contains("topics.regex"));
    }

    @Test
    void apiSecretIsMarkedSensitive() {
        assertEquals(org.apache.kafka.common.config.ConfigDef.Type.PASSWORD,
            new FanInSinkConnector().config().configKeys().get("fanin.target.api.secret").type);
    }

    @Test
    void taskConfigsGetDistinctIndex() {
        FanInSinkConnector connector = new FanInSinkConnector();
        connector.start(base());
        List<Map<String, String>> tasks = connector.taskConfigs(3);
        assertEquals(3, tasks.size());
        assertEquals("2", tasks.get(2).get(FanInSinkConnector.TASK_INDEX));
        assertEquals("fanin-out", tasks.get(0).get("fanin.target.topic"));
    }

    @Test
    void producerConfigsDefaultsAndOverrides() {
        Map<String, String> props = base();
        props.put("fanin.producer.compression.type", "lz4");
        props.put("fanin.producer.acks", "1");
        Map<String, Object> producer = new FanInSinkConfig(props).producerConfigs();
        assertEquals("SASL_SSL", producer.get("security.protocol"));
        assertEquals("PLAIN", producer.get("sasl.mechanism"));
        assertEquals("true", producer.get("enable.idempotence"));
        assertEquals("lz4", producer.get("compression.type"));
        assertEquals("1", producer.get("acks"));
        assertTrue(producer.get("sasl.jaas.config").toString().contains("username=\"key\""));
    }

    @Test
    void secretIsNotPrintedByConfigToString() {
        String text = new FanInSinkConfig(base()).toString();
        assertFalse(text.contains("secret"), text);
    }
}
