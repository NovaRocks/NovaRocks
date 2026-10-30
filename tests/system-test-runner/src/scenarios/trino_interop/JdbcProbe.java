/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for additional
 * information. Licensed under the Apache License, Version 2.0. */
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public final class JdbcProbe {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", args[1]);
        String authority = args[0].replaceFirst("^http://", "");
        try (Connection connection = DriverManager.getConnection("jdbc:trino://" + authority, properties)) {
            if (!connection.isValid(5)) {
                throw new AssertionError("JDBC isValid did not confirm HEAD /v1/statement");
            }
            String version = connection.getMetaData().getDatabaseProductVersion();
            if (version == null || version.isEmpty()) {
                throw new AssertionError("JDBC version read returned no original server version");
            }
            System.out.println("JDBC_PROBE_OK version=" + version);
        }
    }
}
