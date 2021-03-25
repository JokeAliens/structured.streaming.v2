/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn.source;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

/**
 * @author JokeAlien
 * @date 2021/3/25
 */
public class RawStatusScan implements ScanBuilder, Scan {
    private final int partitionNum;
    private final long interval;

    public RawStatusScan() {
        this.partitionNum = 6;
        this.interval = 1;
    }

    public RawStatusScan(int partitionNum, long interval) {
        this.partitionNum = partitionNum;
        this.interval = interval;
    }

    @Override
    public StructType readSchema() {
        return RawStatusProvider.ENCODER.schema();
    }

    @Override
    public Scan build() {
        return this;
    }

    /**
     * 根据对应Table注册的capabilities重载
     *
     * @param checkpointLocation
     * @return
     */
    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new RawStatusMicroBatchStream(partitionNum, interval);
    }
}
