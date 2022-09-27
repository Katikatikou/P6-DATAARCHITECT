import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Example {

    public static void main(String... args) {
        final var connUrl = "jdbc:postgresql://localhost:5432/example?user=postgres&password=password";
        try (var conn = DriverManager.getConnection(connUrl)) {
            System.out.println("Connection successful!!!");
            scaleConnection(conn);
            createSchema(conn);
            insertData(conn);
            executeQueries(conn);
            createContinuousAggregate(conn);
            executeContinuousAggregate(conn);
            compressData(conn);
            deleteData(conn);
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void scaleConnection(final Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                        SELECT add_data_node('datanode_1', 'timescaledb2', password =>'password', if_not_exists => TRUE)
                    """);
        }
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    SELECT add_data_node('datanode_2', 'timescaledb3', password =>'password', if_not_exists => TRUE)
                    """);
        }
    }

    private static void createSchema(final Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DROP TABLE IF EXISTS cpu_data CASCADE
                    """);
        }
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    DROP TABLE IF EXISTS machines CASCADE
                    """);
        }
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE machines (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """);
        }

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE cpu_data (
                        time TIMESTAMPTZ NOT NULL,
                        machine_id INTEGER REFERENCES machines (id),
                        value DOUBLE PRECISION
                    )
                    """);
        }

        try (var stmt = conn.createStatement()) {
            stmt.execute("SELECT create_hypertable('cpu_data', 'time', 'machine_id', 2,  if_not_exists => TRUE)");
        }

        try (var stmt = conn.createStatement()) {
            stmt.execute("SELECT set_chunk_time_interval('cpu_data', INTERVAL '1 day')");
        }
    }

    private static void insertData(final Connection conn) throws SQLException {
        final int numberOfMachines = 10;
        final List<String> machines = new ArrayList<>(numberOfMachines);
        for (int i = 0; i < numberOfMachines; ++i) {
            machines.add(UUID.randomUUID().toString());
        }
        for (final var machine : machines) {
            try (var stmt = conn.prepareStatement("INSERT INTO machines (name) VALUES (?)")) {
                stmt.setString(1, machine);
                stmt.executeUpdate();
            }
        }
        for (int i = 0; i < numberOfMachines; i++) {
            try (var stmt = conn.prepareStatement("""
                    INSERT INTO cpu_data (time, machine_id, value)
                    SELECT g.id,
                           ?,
                           random()
                    FROM generate_series(now() - INTERVAL '1 YEARS', now(), INTERVAL '1 minutes') as g(id)
                    """)) {
                stmt.setInt(1, i + 1);
                stmt.execute();
            }
        }
    }

    private static void executeQueries(final Connection conn) throws SQLException {

        System.out.println("********* Requesting by average");

        try (var stmt = conn.prepareStatement("""
                SELECT time_bucket('1 DAYS', time) AS bucket, avg(value) AS avg, name
                FROM cpu_data
                JOIN machines ON machines.id = cpu_data.machine_id
                GROUP BY bucket, name
                ORDER BY avg DESC
                LIMIT 10
                """)) {

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("%s: %f on %s%n",
                            rs.getTimestamp(1),
                            rs.getDouble(2),
                            rs.getString(3));
                }
            }
        }


        System.out.println("********* Requesting first and last values");

        try (var stmt = conn.prepareStatement("""
                SELECT time_bucket('1 DAYS', time) AS bucket, first(value, time) AS first, last(value, time) as last, name
                FROM cpu_data
                JOIN machines ON machines.id = cpu_data.machine_id
                GROUP BY bucket, name
                ORDER BY bucket DESC
                LIMIT 10
                """)) {

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("%s: %f, %f on %s%n",
                            rs.getTimestamp(1),
                            rs.getDouble(2),
                            rs.getDouble(3),
                            rs.getString(4));
                }
            }
        }
    }

    private static void createContinuousAggregate(final Connection conn) throws SQLException {

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE MATERIALIZED VIEW cpu_consommation_avg_daily
                    WITH (timescaledb.continuous) AS
                        SELECT time_bucket('1 DAYS', time) AS bucket, avg(value) AS avg, machine_id
                        FROM cpu_data
                        GROUP BY bucket, machine_id
                    """);
        }
    }

    private static void executeContinuousAggregate(final Connection conn) throws SQLException {

        System.out.println("********* Exec continuous aggregate");

        try (var stmt = conn.prepareStatement("""
                SELECT bucket, avg, name
                FROM cpu_consommation_avg_daily
                JOIN machines ON machines.id = cpu_consommation_avg_daily.machine_id
                ORDER BY avg DESC
                LIMIT 10
                """)) {

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("%s: %f on %s%n",
                            rs.getTimestamp(1),
                            rs.getDouble(2),
                            rs.getString(3));
                }
            }
        }
    }


    private static void compressData(final Connection conn) throws SQLException {

        System.out.println("Activating compression");

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    ALTER TABLE cpu_data SET (
                            timescaledb.compress,
                            timescaledb.compress_orderby = 'time DESC',
                            timescaledb.compress_segmentby = 'machine_id'
                    )
                    """);
        }

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    SELECT add_compression_policy('cpu_data', INTERVAL '2 weeks')
                    """);
        }

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    SELECT compress_chunk(i, if_not_compressed=>true)
                    FROM show_chunks('cpu_data', older_than => INTERVAL ' 2 weeks') i
                    """);
        }
        try (var stmt = conn.prepareStatement("""
                SELECT pg_size_pretty(before_compression_total_bytes) as "before compression",
                pg_size_pretty(after_compression_total_bytes) as "after compression"
                FROM hypertable_compression_stats('cpu_data')
                """)) {
            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("before %s: after %s%n",
                            rs.getString(1),
                            rs.getString(2));
                }
            }
        }

    }

    private static void deleteData(Connection conn) throws SQLException {
        try (var stmt = conn.prepareStatement("""
                SELECT count(time) FROM cpu_data
                """)) {
            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("before deleting data %s%n",
                            rs.getString(1));
                }
            }
        }

        try (var stmt = conn.prepareStatement("""
                SELECT add_retention_policy('cpu_data', INTERVAL '1 MONTHS');
                """)) {
            stmt.execute();
        }

        try (var stmt = conn.prepareStatement("""
                SELECT drop_chunks('cpu_data', INTERVAL '1 MONTHS');
                """)) {
            stmt.execute();
        }

        try (var stmt = conn.prepareStatement("""
                SELECT count(time) FROM cpu_data
                """)) {
            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("after deleting data %s%n",
                            rs.getString(1));
                }
            }
        }
    }
}