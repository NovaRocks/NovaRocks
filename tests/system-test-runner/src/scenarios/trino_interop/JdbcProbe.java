/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for additional
 * information. Licensed under the Apache License, Version 2.0. */
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Optional;
import io.trino.jdbc.TrinoConnection;

public final class JdbcProbe {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", args[1]);
        properties.setProperty("validateConnection", "true");
        String authority = args[0].replaceFirst("^http://", "");
        try (Connection connection = DriverManager.getConnection("jdbc:trino://" + authority, properties)) {
            if (!connection.isValid(5)) {
                throw new AssertionError("JDBC isValid did not confirm HEAD /v1/statement");
            }
            Optional<String> version = connection.unwrap(TrinoConnection.class).getServerVersion();
            if (version.isEmpty() || version.get().isEmpty()) {
                throw new AssertionError("JDBC /v1/info returned no original server version");
            }
            System.out.println("JDBC_PROBE_OK validated=true server_version=" + version.get());
        }
    }
}
