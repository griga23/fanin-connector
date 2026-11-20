package com.example.fanin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Connector version, read from the Maven-filtered version.properties resource. */
public final class Version {

    private static final String VERSION = load();

    private Version() {
    }

    public static String get() {
        return VERSION;
    }

    private static String load() {
        try (InputStream in = Version.class.getResourceAsStream("/version.properties")) {
            if (in == null) {
                return "unknown";
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("version", "unknown");
        } catch (IOException e) {
            return "unknown";
        }
    }
}
