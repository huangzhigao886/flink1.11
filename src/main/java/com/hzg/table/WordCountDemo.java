package com.hzg.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class WordCountDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        String wordPath = WordCountDemo.class.getClassLoader().getResource("wordCount.txt").getPath();

        //第一种注册table的办法
        tableEnv.connect(new FileSystem().path(wordPath)).withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING)).createTemporaryTable("fileSource");
        //第二种通过实现tableSource的方式
//        TableSource tableSource = new CsvTableSource(wordPath,new String[]{"word"},new TypeInformation[]{Types.STRING});
//        tableEnv.registerTableSource("fileSource",tableSource);

        //第三种通过将数据流转换成table的方式
//        tableEnv.registerDataSet("fileSource",DataSet);

        Table result = tableEnv.scan("fileSource").groupBy("word").select("word,count(1) as count");
        tableEnv.toDataSet(result, Row.class).print();

    }
}
