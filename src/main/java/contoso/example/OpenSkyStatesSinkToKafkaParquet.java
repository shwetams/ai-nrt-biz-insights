package contoso.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import contoso.example.util.CLI;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;

public class OpenSkyStatesSinkToKafkaParquet {

    public static final RowType rowType = new RowType(Arrays.asList(
        new RowType.RowField("states", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("time",new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("time_position",new VarCharType(VarCharType.MAX_LENGTH))
    ));

    String[] deltaPartitionKeys = {"time_position"};
    
    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);
        // 1. set stream environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = "10.0.0.38:9092,10.0.0.39:9092,10.0.0.40:9092";
        env.enableCheckpointing(1000);
        // Create a counter stream
        DataStream<Long> openSkyStream = env.addSource(new CounterSource()).name("Counter");

        // 3. Retrieve OpenSky
        //DataStream<String> openSkyStream = env.addSource(new OpenSkySource()).map(new OpenSkyToKafkaMapper());

        //DataStream<String> openSkyStream = env.fromElements("Hello", "World", "Hello", "Flink", "World");

//        // 4. sink OpenSky State into kafka
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(brokers)
//                .setProperty("transaction.timeout.ms", "900000")
//                .setProperty("max.request.size", "104857600")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("airplanes_state_vectors2")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .build();
//
//        openSkyStream.sinkTo(sink);


        // Get the current timestamp
        LocalDateTime currentTime = LocalDateTime.now();

        // Define the desired date-time format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Format the current LocalDateTime to a string
        String currentTimeString = currentTime.format(formatter);

        DateTimeFormatter formatterMin = DateTimeFormatter.ofPattern("mm");
        String currentMinute = currentTime.format(formatterMin);
        
//        openSkyStream.sinkTo(
//            FileSink.<String>forRowFormat(
//                params.getOutput().get(), new SimpleStringEncoder<>())
//                .withRollingPolicy(
//                    DefaultRollingPolicy.builder()
//                        .withMaxPartSize((long)(1l << 20))
//                        .withRolloverInterval(1)
//                        .build())
//                .build())
//            .name("file-sink");

        // 3. Transform the OpenSky data to Delta format
        //DataStream<RowData> records = openSkyStream.flatMap(new OpenSkyToDeltaMapper()).name("OpenSky to Delta");

        DataStream<RowData> records = openSkyStream.map(inputStr -> {
            GenericRowData row = new GenericRowData(3);

            row.setField(0, StringData.fromString(inputStr.toString()));
            row.setField(1, StringData.fromString(currentMinute));
            row.setField(2, StringData.fromString(currentTimeString));
            return (RowData) row;
        });
        //String uri = "file:////etc/hadoop/conf/core-site.xml";
        //URI uriRes = URI.create(uri);
        Configuration hadoopConfig = new Configuration();
        //hadoopConfig.addResource(uriRes.toString());
        String[] partitionCols = {"time"};
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                params.getOutput().get(),
                hadoopConfig,
                rowType)
            .withPartitionColumns(partitionCols)
            .build();
        records.sinkTo(deltaSink);
        // 5. execute the stream
        env.execute("OpenSky State Sink to Kafka");
    }

    private static class OpenSkySource implements SourceFunction<String> {
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
                Thread.sleep(20000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
}

    // Custom SourceFunction implementation to emit a continuously incrementing counter
    public static class CounterSource implements SourceFunction<Long> {
        private volatile boolean isRunning = true;
        private long counter = 0;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                // Emit the current counter value
                ctx.collect(counter);

                // Increment the counter
                counter++;

                // Sleep for 1 second
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }



    private static class OpenSkyToKafkaMapper implements MapFunction<String, String> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String map(String value) throws Exception {
            ObjectNode root = objectMapper.readValue(value, ObjectNode.class);
            StringBuilder sb = new StringBuilder();
            JsonNode statesNode = root.get("states");
            // Get the current timestamp
            LocalDateTime currentTime = LocalDateTime.now();

            // Format the current timestamp to display only the hour part
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH");
            String currentHour = currentTime.format(formatter);

            DateTimeFormatter formatterMin = DateTimeFormatter.ofPattern("mm");
            String currentMinute = currentTime.format(formatterMin);
            JSONObject stateVal = new JSONObject();
            JSONArray statesArr = new JSONArray();
            if (statesNode != null && !statesNode.isNull()) {
                ArrayNode states = (ArrayNode) statesNode;
                for (int i = 0; i < states.size(); i++) {
                    ArrayNode state = (ArrayNode) states.get(i);
                    ObjectNode transformedState = objectMapper.createObjectNode();
                    //java.sql.Timestamp timestamp = new java.sql.Timestamp(getLong(state.get(3)));
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
                    transformedState.put("category", getInt(state.get(17)));
                    //transformedState.put("time", String.valueOf(timestamp));
                    statesArr.add(transformedState);
                    sb.append(transformedState.toString()).append("\n");
                }
            }
            JSONObject output = new JSONObject();
            output.put("states", statesArr);
            output.put("time", currentMinute);
            
            System.out.println(output.toString());
            return output.toString();
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
