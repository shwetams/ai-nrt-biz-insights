package contoso.example;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import contoso.example.util.CLI;
import contoso.example.util.WordCountData;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import contoso.example.util.CLI;
import contoso.example.util.WordCountData;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.example.flink.delta.util.CLI;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class OpenSkyFlightsIntervalToKafka {

    public static final RowType rowType = new RowType(Arrays.asList(
        new RowType.RowField("states", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("time",new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("time_position",new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("interval",new VarCharType(VarCharType.MAX_LENGTH))
    ));

    public static final int interval = 6000;
    
    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);
        // 1. stream env prepare
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = "10.0.0.38:9092,10.0.0.39:9092,10.0.0.40:9092";
        env.enableCheckpointing(interval*1000);

        // Get the current timestamp
        LocalDateTime currentTime = LocalDateTime.now();

        // Define the desired date-time format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Format the current LocalDateTime to a string
        String currentTimeString = currentTime.format(formatter);

        DateTimeFormatter formatterMin = DateTimeFormatter.ofPattern("mm");
        String currentMinute = currentTime.format(formatterMin);

        DateTimeFormatter formatterHour = DateTimeFormatter.ofPattern("HH");

        // Format the current LocalDateTime to a string with only the hour part
        String currentHour = currentTime.format(formatterHour);

        // 3. Retrieve OpenSky Flights
        DataStream<String> openSkyStream = env.addSource(new OpenSkySourceFunction());

        // 4. sink OpenSky Flights in Time Interval into kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setProperty("transaction.timeout.ms", "900000")
                .setProperty("max.request.size", "104857600")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flights_in_time_interval")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        DataStream<RowData> records = openSkyStream.map(inputStr -> {
            GenericRowData row = new GenericRowData(4);

            row.setField(0, StringData.fromString(inputStr));
            row.setField(1, StringData.fromString(currentHour));
            row.setField(2, StringData.fromString(currentTimeString));
            row.setField(3, StringData.fromString(String.valueOf(interval)));
            return (RowData) row;
        });
        String uri = "file:////etc/hadoop/conf/core-site.xml";
        URI uriRes = URI.create(uri);
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.addResource(uriRes.toString());
        String[] partitionCols = {"time"};
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                params.getOutput().get(),
                hadoopConfig,
                rowType)
            .withPartitionColumns(partitionCols)
            .build();
        records.sinkTo(deltaSink);

//        openSkyStream.sinkTo(
//            FileSink.<String>forRowFormat(
//                params.getOutput().get(), new SimpleStringEncoder<>())
//                .withRollingPolicy(
//                    DefaultRollingPolicy.builder()
//                        .withMaxPartSize((long)(1l << 20))
//                        .withRolloverInterval(10)
//                        .build())
//                .build())
//            .name("file-sink");
//
//        openSkyStream.sinkTo(sink);

        env.execute("OpenSky Flights in Time Interval Sink to Kafka");
    }

    private static class OpenSkySourceFunction implements SourceFunction<String> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                long now = Instant.now().getEpochSecond();
                long oneHourAgo = now - interval;

                URL url = new URL("https://opensky-network.org/api/flights/all?begin=" + oneHourAgo + "&end=" + now);
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

                // Split the array of flights into individual elements
                JSONParser parser = new JSONParser();
                JSONArray flights = (JSONArray) parser.parse(response.toString());
//                for (int i = 0; i < flights.size(); i++) {
//                    JSONObject flight = (JSONObject) flights.get(i);
//                    ctx.collect(flight.toString());
//                }
                ctx.collect(flights.toString());

                // Sleep for one hour before retrieving data again
                
                Thread.sleep(interval*1000);
            }
        }   

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}

