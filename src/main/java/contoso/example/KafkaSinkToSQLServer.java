package contoso.example;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Types;

public class KafkaSinkToSQLServer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = "10.0.0.38:9092,10.0.0.39:9092,10.0.0.40:9092";
        // Set up the Kafka source
        String kafkaTopic = "airplanes_state_vectors2";
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStream.print();
        // Set up the Azure SQL sink
        String dbUrl = "jdbc:sqlserver://contosoflinksqlserver.database.windows.net:1433;database=inventory";
        String dbUsername = "dbadmin";
        String dbPassword = "Password01!";

        kafkaStream.addSink(JdbcSink.sink(
                "INSERT INTO dbo.airplanes_state_vectors (icao24, callsign, origin_country, time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    // Parse the JSON record
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode root = null;
                    try {
                        root = objectMapper.readTree(record);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    // Set the prepared statement parameters
                    ps.setString(1, root.get("icao24").asText());
                    ps.setString(2, root.get("callsign").asText());
                    ps.setString(3, root.get("origin_country").asText());
                    ps.setLong(4, root.get("time_position").asLong());
                    ps.setLong(5, root.get("last_contact").asLong());
                    ps.setDouble(6, root.get("longitude").asDouble());
                    ps.setDouble(7, root.get("latitude").asDouble());
                    ps.setDouble(8, root.get("baro_altitude").asDouble());
                    ps.setBoolean(9, root.get("on_ground").asBoolean());
                    ps.setDouble(10, root.get("velocity").asDouble());
                    ps.setDouble(11, root.get("true_track").asDouble());
                    ps.setDouble(12, root.get("vertical_rate").asDouble());
                    // Handle the sensors array
                    ArrayNode sensorsNode = (ArrayNode) root.get("sensors");
                    if (sensorsNode != null && !sensorsNode.isNull()) {
                        Integer[] sensors = new Integer[sensorsNode.size()];
                        for (int i = 0; i < sensorsNode.size(); i++) {
                            sensors[i] = sensorsNode.get(i).asInt();
                        }
                        ps.setArray(13, ps.getConnection().createArrayOf("INTEGER", sensors));
                    } else {
                        ps.setNull(13, Types.ARRAY);
                    }
                    ps.setDouble(14, root.get("geo_altitude").asDouble());
                    ps.setString(15, root.get("squawk").asText());
                    ps.setBoolean(16, root.get("spi").asBoolean());
                    ps.setInt(17, root.get("position_source").asInt());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .withUsername(dbUsername)
                        .withPassword(dbPassword)
                        .build()));

        env.execute("Kafka to Azure SQL");
    }
}
