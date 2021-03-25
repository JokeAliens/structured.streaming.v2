/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn.source;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Set;

/**
 * @author JokeAlien
 * @date 2021/3/25
 */
public class RawStatusTable implements SupportsRead {
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        // 需要的参数
        int partitionNum =
                caseInsensitiveStringMap.getInt("partition_num", 5);

        long interval =
                caseInsensitiveStringMap.getLong("interval", 1);
        // ...

        return new RawStatusScan(partitionNum, interval);
    }

    @Override
    public String name() {
        return RawStatusTable.class.getSimpleName();
    }

    @Override
    public StructType schema() {
        return RawStatusProvider.ENCODER.schema();
    }

    /**
     * micro-batch 或 continuous
     *
     * @return
     */
    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.MICRO_BATCH_READ);
    }
}
