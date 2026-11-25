/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for additional
 * information. Licensed under the Apache License, Version 2.0. */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
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
            jsonResults(connection);
        }
        for (boolean explicit : new boolean[] {true, false}) {
            properties.setProperty("explicitPrepare", Boolean.toString(explicit));
            try (Connection connection = DriverManager.getConnection("jdbc:trino://" + authority, properties)) {
                preparedReads(connection);
                preparedWrite(connection, args[2], explicit ? 51 : 52);
                System.out.println("JDBC_PREPARED_OK explicitPrepare=" + explicit);
            }
        }
        try (Connection connection = DriverManager.getConnection("jdbc:trino://" + authority, properties);
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT id FROM " + args[2] + " ORDER BY id")) {
            check(rows.next() && rows.getLong(1) == 51, "explicit prepared INSERT readback");
            check(rows.next() && rows.getLong(1) == 52 && !rows.next(), "immediate prepared INSERT readback");
        }
    }

    private static void jsonResults(Connection connection) throws Exception {
        String document = "{\"b\":2,\"a\":1,\"a\":2}";
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT JSON '{"b":2,"a":1,"a":2}' AS doc,
                               CAST(NULL AS JSON) AS sql_null,
                               JSON 'null' AS json_null,
                               ARRAY[JSON '{"b":2,"a":1,"a":2}', CAST(NULL AS JSON), JSON 'null'] AS nested
                        """)) {
            var metadata = rows.getMetaData();
            for (int column = 1; column <= 3; column++) {
                check(metadata.getColumnType(column) == Types.JAVA_OBJECT, "JSON JDBC type");
                check(metadata.getColumnTypeName(column).equals("json"), "JSON SQL type");
                check(metadata.getColumnClassName(column).equals(String.class.getName()), "JSON JDBC class");
            }
            check(metadata.getColumnType(4) == Types.ARRAY, "array(json) JDBC type");
            check(metadata.getColumnTypeName(4).equals("array(json)"), "array(json) SQL type");
            check(rows.next(), "missing JSON result row");
            check(document.equals(rows.getObject(1)) && !rows.wasNull(), "direct JSON text and duplicates");
            check(rows.getObject(2) == null && rows.wasNull(), "JSON column SQL NULL");
            check("null".equals(rows.getString(3)) && !rows.wasNull(), "present JSON null text");
            var nested = rows.getArray(4);
            try {
                check(nested.getBaseType() == Types.JAVA_OBJECT
                        && nested.getBaseTypeName().equals("json"), "nested JSON element type");
                check(Arrays.equals((Object[]) nested.getArray(), new Object[] {document, null, "null"}),
                        "nested JSON text and nulls");
            }
            finally {
                nested.free();
            }
            check(!rows.next(), "unexpected additional JSON result row");
        }
        System.out.println("JDBC_JSON_OK direct=true nested=true sql_null=true json_null=true");
    }

    private static void preparedReads(Connection connection) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT CAST(? AS BIGINT) AS n, CAST(? AS VARCHAR(4)) AS text_value LIMIT ?")) {
            var parameters = statement.getParameterMetaData();
            check(parameters.getParameterCount() == 3, "parameter metadata count");
            check(parameters.getParameterTypeName(1).equals("unknown"), "first input type");
            check(parameters.getParameterTypeName(2).equals("unknown"), "second input type");
            check(parameters.getParameterTypeName(3).equals("bigint"), "row count input type");
            var metadata = statement.getMetaData();
            check(metadata.getColumnCount() == 2, "output metadata count");
            check(metadata.getColumnTypeName(1).equals("bigint"), "BIGINT output type");
            check(metadata.getColumnTypeName(2).equals("varchar(4)"), "VARCHAR output type");
            check(metadata.getPrecision(2) == 4, "VARCHAR output precision");
            statement.setLong(1, 41);
            statement.setString(2, "é");
            statement.setLong(3, 1);
            try (ResultSet rows = statement.executeQuery()) {
                check(rows.next() && rows.getLong(1) == 41 && rows.getString(2).equals("é") && !rows.next(),
                        "first prepared execution");
            }
            statement.setNull(1, Types.BIGINT);
            statement.setString(2, "abc");
            try (ResultSet rows = statement.executeQuery()) {
                check(rows.next() && rows.getObject(1) == null && rows.getString(2).equals("abc") && !rows.next(),
                        "repeated prepared execution with typed NULL");
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1 AS one")) {
            check(statement.getParameterMetaData().getParameterCount() == 0, "zero-parameter metadata");
            check(statement.getMetaData().getColumnCount() == 1, "zero-parameter output metadata");
        }
    }

    private static void preparedWrite(Connection connection, String table, long value) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
            check(statement.getParameterMetaData().getParameterTypeName(1).equals("unknown"), "INSERT input metadata");
            check(statement.getMetaData().getColumnTypeName(1).equals("bigint"), "INSERT rows metadata");
            statement.setLong(1, value);
            check(statement.executeUpdate() == 1, "prepared INSERT update count");
        }
    }

    private static void check(boolean condition, String purpose) {
        if (!condition) {
            throw new AssertionError(purpose);
        }
    }
}
