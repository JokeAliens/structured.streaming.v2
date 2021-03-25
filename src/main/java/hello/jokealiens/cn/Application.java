/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.Trigger;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author JokeAlien
 * @date 2021/3/24
 */
public class Application {

    private static long BATCH_INTERVAL = 500;

    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        sparkSession
                .readStream()
                .format("hello.jokealiens.cn.source.RawStatusProvider")
                .option("partition_num", 3)
                .option("interval", BATCH_INTERVAL)
                .load()
                .withColumn("eventTime", functions.current_timestamp())
                .withWatermark("eventTime", "1 hour")
                .groupBy(functions.window(functions.column("eventTime"), "30 minutes"),
                        functions.column("username"),
                        functions.column("hostname"))
                .agg(functions.udaf(new StatusReduceFunc(), Encoders.bean(Status.class))
                                .apply(functions.column("reportTime"),
                                        functions.column("status")).as("status"),
                        functions.min("reportTime").as("earliestTime"),
                        functions.max("reportTime").as("latestTime"),
                        functions.count("username").as("aggCount"))
                .withColumn("onlineTime",
                        functions.col("latestTime")
                                .minus(functions.col("earliestTime")))
                .writeStream()
                .outputMode("complete")
                .format("console")
                .trigger(Trigger.ProcessingTime(BATCH_INTERVAL, TimeUnit.MILLISECONDS))
                .start()
                .awaitTermination();
    }

    /**
     * 必须 public,
     * input.class, buf.class, result.class
     */
    public static class StatusReduceFunc extends Aggregator<Status, Status, String> {

        /**
         * 初始化buf
         *
         * @return
         */
        @Override
        public Status zero() {
            return new Status(0, "OFFLINE");
        }

        /**
         * 用in初始化buf
         *
         * @return
         */
        @Override
        public Status reduce(Status buf, Status in) {
            buf.setReportTime(in.getReportTime());
            buf.setStatus(in.getStatus());
            return buf;
        }

        /**
         * 两个buf reduce
         *
         * @param buf1
         * @param buf2
         * @return
         */
        @Override
        public Status merge(Status buf1, Status buf2) {
            return buf1.reportTime > buf2.reportTime ? buf1 : buf2;
        }

        /**
         * buf转result
         *
         * @param reduction
         * @return
         */
        @Override
        public String finish(Status reduction) {
            return reduction.status;
        }

        @Override
        public Encoder<Status> bufferEncoder() {
            return Encoders.bean(Status.class);
        }

        @Override
        public Encoder<String> outputEncoder() {
            return Encoders.STRING();
        }
    }

    /**
     * 必须 public
     */
    public static class Status implements Serializable {
        private long reportTime;
        private String status;

        public Status() {
        }

        public Status(long reportTime, String status) {
            this.reportTime = reportTime;
            this.status = status;
        }

        public long getReportTime() {
            return reportTime;
        }

        public void setReportTime(long reportTime) {
            this.reportTime = reportTime;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }
}
