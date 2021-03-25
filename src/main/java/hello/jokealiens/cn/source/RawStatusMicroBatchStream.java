/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn.source;

import hello.jokealiens.cn.bean.RawStatusBean;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

import java.io.IOException;

/**
 * 假设上游数据获取需要时间参数
 *
 * @author JokeAlien
 * @date 2021/3/25
 */
public class RawStatusMicroBatchStream implements MicroBatchStream {
    private final int partitionNum;
    private final long interval;
    private final long initialTime = System.currentTimeMillis();

    public RawStatusMicroBatchStream() {
        this.partitionNum = 6;
        this.interval = 1;
    }

    public RawStatusMicroBatchStream(int partitionNum, long interval) {
        this.partitionNum = partitionNum;
        this.interval = interval;
    }

    @Override
    public Offset latestOffset() {
        return new LongOffset(System.currentTimeMillis() + interval);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        long startTime = ((LongOffset) start).offset();
        long endTime = ((LongOffset) end).offset();

        InputPartition[] inputPartitions = new InputPartition[this.partitionNum];
        for (int i = 0; i < this.partitionNum; i++) {
            // 数据在此处获取，或者将获取参数传递到RawStatusPartition中，在里面处理
            String sourcesOrParameter = "get data from" + startTime + "to" + endTime;
            inputPartitions[i] = new RawStatusPartition(sourcesOrParameter);
        }
        return inputPartitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return RawStatusPartitionReaderFactory.instance;
    }

    @Override
    public Offset initialOffset() {
        return new LongOffset(initialTime);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return LongOffset.apply(Long.parseLong(json));
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {

    }

    private static class RawStatusPartition implements InputPartition {
        private String sourceOrParameter;

        public RawStatusPartition(String sourceOrParameter) {
            this.sourceOrParameter = sourceOrParameter;
        }

        public String getSourceOrParameter() {
            return sourceOrParameter;
        }

        public void setSourceOrParameter(String sourceOrParameter) {
            this.sourceOrParameter = sourceOrParameter;
        }
    }

    private static class RawStatusPartitionReaderFactory implements PartitionReaderFactory {
        public static RawStatusPartitionReaderFactory instance = new RawStatusPartitionReaderFactory();

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            RawStatusPartition rawStatusPartition = (RawStatusPartition) partition;
            String sourceOrParameter = rawStatusPartition.getSourceOrParameter();
            return new RawStatusPartitionReader(sourceOrParameter);
        }
    }

    private static class RawStatusPartitionReader implements PartitionReader<InternalRow> {
        private final String sourceOrParameter;

        private int index = -1;

        public RawStatusPartitionReader(String sourceOrParameter) {
            this.sourceOrParameter = sourceOrParameter;
        }

        @Override
        public boolean next() throws IOException {
            // 假设：生成一个数据就不再read
            return 0 > index++;
        }

        @Override
        public InternalRow get() {
            return RawStatusProvider
                    .ENCODER
                    .createSerializer()
                    .apply(new RawStatusBean(sourceOrParameter));
        }

        @Override
        public void close() throws IOException {

        }
    }
}
