import org.apache.kafka.clients.consumer.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;

import org.json.JSONObject;

public class KafkaAdsbConsumer {
    private static final int BATCH_SIZE = 50;
    private static final int FLUSH_INTERVAL_MS = 100; // push to DB every 100ms or push 50 msgs whichever comes first
    private static final List<JSONObject> messageBuffer = new ArrayList<>();

    public static void main(String[] args) {
        String topic = "adsb";
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "adsb-consumer-group";

        // Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("🔥 Listening to Kafka topic: " + topic);

        String jdbcURL = "jdbc:postgresql://localhost:5432/adsb";
        String dbUser = "user";
        String dbPassword = "password";

        try (Connection conn = DriverManager.getConnection(jdbcURL, dbUser, dbPassword)) {
            conn.setAutoCommit(false);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    messageBuffer.add(new JSONObject(record.value()));

                    // push to db if buffer full
                    if (messageBuffer.size() >= BATCH_SIZE) {
                        insertBatch(conn);
                    }
                }

                if (!messageBuffer.isEmpty()) {
                    Thread.sleep(FLUSH_INTERVAL_MS);
                    insertBatch(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private static void insertBatch(Connection conn) throws SQLException {
        if (messageBuffer.isEmpty()) return;

        String insertAircraftSQL = "INSERT INTO aircraft (hex, first_seen, is_military, is_government, is_helicopter, is_other_aircraft) " +
                                   "VALUES (?, NOW(), ?, ?, ?, ?) ON CONFLICT (hex) DO NOTHING";

        String insertPositionSQL = "INSERT INTO adsb_positions " +
                                   "(hex, timestamp, position, altitude, speed, heading, vertical_rate, squawk, signal_strength, data_sources, is_military, is_government, is_helicopter, is_other_aircraft, distance) " +
                                   "VALUES (?, NOW(), ST_SetSRID(ST_MakePoint(?, ?), 4326), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                                   "ST_DistanceSphere(ST_SetSRID(ST_MakePoint(?, ?), 4326), ST_SetSRID(ST_MakePoint(MY_LON, MY_LAT), 4326)))"; //this requires lon, lat format

        try (PreparedStatement insertAircraftStmt = conn.prepareStatement(insertAircraftSQL);
             PreparedStatement insertPositionStmt = conn.prepareStatement(insertPositionSQL)) {

            for (JSONObject json : messageBuffer) {
                String hex = json.optString("hex", "UNKNOWN").replaceAll("[^0-9a-fA-F]", ""); //sometimes hex gets passed in with ~ and it breaks everything
                int altitude = json.optInt("altitude", 0);
                int speed = json.optInt("gs", 0);
                int heading = json.optInt("track", 0);
                int verticalRate = json.optInt("vertical_rate", 0);
                String squawk = json.optString("squawk", "0000");
                boolean isMilitary = json.optBoolean("isMilitary", false);
                boolean isGovernment = json.optBoolean("isGovernment", false);
                boolean isHelicopter = json.optBoolean("isHelicopter", false);
                boolean isOtherAircraft = json.optBoolean("isOtherAircraft", false);
                double signalStrength = json.optDouble("rssi", -999.0);
                List<String> dataSources = new ArrayList<>();

                if (json.has("mlat") && json.getJSONArray("mlat").length() > 0) dataSources.add("mlat");
                if (json.has("adsb") && json.getJSONArray("adsb").length() > 0) dataSources.add("adsb");
                if (json.has("tisb") && json.getJSONArray("tisb").length() > 0) dataSources.add("tisb");

                double lat = json.has("lat") ? json.optDouble("lat") : Double.NaN;
                double lon = json.has("lon") ? json.optDouble("lon") : Double.NaN;

                insertAircraftStmt.setString(1, hex);
                insertAircraftStmt.setBoolean(2, isMilitary);
                insertAircraftStmt.setBoolean(3, isGovernment);
                insertAircraftStmt.setBoolean(4, isHelicopter);
                insertAircraftStmt.setBoolean(5, isOtherAircraft);
                insertAircraftStmt.addBatch();

                insertPositionStmt.setString(1, hex);
                insertPositionStmt.setDouble(2, lon);
                insertPositionStmt.setDouble(3, lat);
                insertPositionStmt.setInt(4, altitude);
                insertPositionStmt.setInt(5, speed);
                insertPositionStmt.setInt(6, heading);
                insertPositionStmt.setInt(7, verticalRate);
                insertPositionStmt.setString(8, squawk);
                insertPositionStmt.setDouble(9, signalStrength);
                insertPositionStmt.setArray(10, conn.createArrayOf("TEXT", dataSources.toArray()));
                insertPositionStmt.setBoolean(11, isMilitary);
                insertPositionStmt.setBoolean(12, isGovernment);
                insertPositionStmt.setBoolean(13, isHelicopter);
                insertPositionStmt.setBoolean(14, isOtherAircraft);

                // Distance from my antenna
                if (!Double.isNaN(lat) && !Double.isNaN(lon)) {
                    insertPositionStmt.setDouble(15, lon);
                    insertPositionStmt.setDouble(16, lat);
                } else {
                    insertPositionStmt.setNull(15, java.sql.Types.DOUBLE);
                    insertPositionStmt.setNull(16, java.sql.Types.DOUBLE);
                }

                insertPositionStmt.addBatch();
            }

            insertAircraftStmt.executeBatch();
            insertPositionStmt.executeBatch();
            conn.commit();
            messageBuffer.clear();
        } catch (SQLException e) {
            conn.rollback();
            e.printStackTrace();
        }
    }
}

