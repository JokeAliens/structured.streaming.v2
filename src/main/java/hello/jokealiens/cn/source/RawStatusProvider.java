/*
 * Copyright (c) 2021. JokeAliens
 */

package hello.jokealiens.cn.source;

import hello.jokealiens.cn.bean.RawStatusBean;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Data Source v2 一般实现TableProvider
 *
 * @author JokeAlien
 * @date 2021/3/25
 */
public class RawStatusProvider implements DataSourceRegister, TableProvider {
    //
    public static final ExpressionEncoder<RawStatusBean> ENCODER = ExpressionEncoder.javaBean(RawStatusBean.class);

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return ENCODER.schema();
    }

    @Override
    public Table getTable(StructType structType, Transform[] transforms, Map<String, String> map) {
        return new RawStatusTable();
    }

    @Override
    public String shortName() {
        return RawStatusProvider.class.getSimpleName();
    }
}
