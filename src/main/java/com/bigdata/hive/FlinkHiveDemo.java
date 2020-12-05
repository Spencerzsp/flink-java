package com.bigdata.hive;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ author spencer
 * @ date 2020/6/12 15:28
 */
public class FlinkHiveDemo {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir     = "file:///D:\\IdeaProjects\\flink-java\\src\\main\\resources\\cluster\\hive-site.xml"; // a local path
        String version         = "1.1.0";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
        tableEnv.useCatalog("myhive");

        Table table = tableEnv.sqlQuery("select * from person");

        tableEnv.execute("FlinkHiveDemo");

    }
}
