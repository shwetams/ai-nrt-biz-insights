package contoso.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;


public class OpenSkyToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = "10.0.0.38:9092,10.0.0.39:9092,10.0.0.40:9092";
        // Set up the OpenSky source
        DataStream<String> openSkyStream = env.addSource(new OpenSkySource());

        // Transform the data into a JSON format
        DataStream<String> jsonStream = openSkyStream.map(new OpenSkyToJsonMapper());

        // 4. sink OpenSky State into kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setProperty("transaction.timeout.ms", "900000")
                .setProperty("max.request.size", "104857600")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("airplanes_state_vectors2")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        jsonStream.sinkTo(sink);

        env.execute("OpenSky to Kafka");
    }

    private static class OpenSkySource extends RichSourceFunction<String> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                URL url = new URL("https://opensky-network.org/api/states/all");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                String auth = "cicicao:Password01!";
                conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes()));

                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                ctx.collect(response.toString());

                // Sleep for one hour before retrieving data again
                Thread.sleep(60000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    private static class OpenSkyToJsonMapper implements MapFunction<String, String> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String map(String value) throws Exception {
            ObjectNode root = objectMapper.readValue(value, ObjectNode.class);
            StringBuilder sb = new StringBuilder();
            ArrayNode states = (ArrayNode) root.get("states");
            if (states != null && !states.isNull()) {
                for (int i = 0; i < states.size(); i++) {
                    ArrayNode state = (ArrayNode) states.get(i);
                    ObjectNode transformedState = objectMapper.createObjectNode();
                    transformedState.put("icao24", getText(state.get(0)));
                    transformedState.put("callsign", getText(state.get(1)));
                    transformedState.put("origin_country", getText(state.get(2)));
                    transformedState.put("time_position", getLong(state.get(3)));
                    transformedState.put("last_contact", getLong(state.get(4)));
                    transformedState.put("longitude", getDouble(state.get(5)));
                    transformedState.put("latitude", getDouble(state.get(6)));
                    transformedState.put("baro_altitude", getDouble(state.get(7)));
                    transformedState.put("on_ground", getBoolean(state.get(8)));
                    transformedState.put("velocity", getDouble(state.get(9)));
                    transformedState.put("true_track", getDouble(state.get(10)));
                    transformedState.put("vertical_rate", getDouble(state.get(11)));
                    transformedState.set("sensors", state.get(12));
                    transformedState.put("geo_altitude", getDouble(state.get(13)));
                    transformedState.put("squawk", getText(state.get(14)));
                    transformedState.put("spi", getBoolean(state.get(15)));
                    transformedState.put("position_source", getInt(state.get(16)));

                    sb.append(transformedState.toString()).append("\n");
                }
            }
            return sb.toString();
        }

        private String getText(JsonNode node) {
            return node != null && !node.isNull() ? node.asText() : null;
        }

        private Long getLong(JsonNode node) {
            return node != null && !node.isNull() ? node.asLong() : null;
        }

        private Double getDouble(JsonNode node) {
            return node != null && !node.isNull() ? node.asDouble() : null;
        }

        private Boolean getBoolean(JsonNode node) {
            return node != null && !node.isNull() ? node.asBoolean() : null;
        }

        private Integer getInt(JsonNode node) {
            return node != null && !node.isNull() ? node.asInt() : null;
        }
    }
}
