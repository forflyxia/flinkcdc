package com.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 *
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class App {
    public static void main(String[] args) {
        new Thread(() -> {
            try {
                MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                        .hostname("localhost")
                        .port(3306)
                        .serverTimeZone("Asia/Shanghai")
                        .databaseList("db1") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                        .tableList("db1.t1") // set captured table
                        .username("root")
                        .scanNewlyAddedTableEnabled(true)
                        .password("123456")
                        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                        .startupOptions(StartupOptions.initial())
                        .build();

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // enable checkpoint
                env.enableCheckpointing(3000);

                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                        // set 4 parallel source tasks
                        .setParallelism(4)
                        .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

                env.execute("Print MySQL Snapshot + Binlog");

            } catch (Exception ex) {

            }
        }).start();
        SpringApplication.run(App.class, args);
    }
}
