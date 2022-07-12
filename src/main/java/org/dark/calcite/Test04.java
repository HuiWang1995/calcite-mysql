package org.dark.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * 测试 framework
 */
public class Test04 {

    public static Logger logger = LoggerFactory.getLogger(Test04.class);


    // single schema query
    public static void main(String[] args) throws Exception {

        Properties info;
        Connection connection;
        Statement statement;
        ResultSet resultSet = null;

        // 构造Schema
        RestClient restClient = RestClient
                .builder(new HttpHost("aaa.club", 9200))        // es默认9200端口
                .build();
        // index =  "kibana_sample_data_logs"
        ElasticsearchSchema esSchema = new ElasticsearchSchema(restClient, new ObjectMapper(), "kibana_sample_data_logs");
        // 设置连接参数
        info = new Properties();
        info.setProperty("caseSensitive", "false");        // SQL大小写不敏感
        // 建立连接
        connection = DriverManager.getConnection("jdbc:calcite:", info);
        // 取得Calcite连接
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        // 取得RootScheam RootSchema是所有Schema的父Schema
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // 添加schema
        rootSchema.add("es", esSchema);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();
        Planner planner = Frameworks.getPlanner(config);


        // 编写SQL
        String sql = "select _MAP['memory'],_MAP['event.dataset'],_MAP['IP'] from \"ES\".\"kibana_sample_data_logs\" " +
                " WHERE _MAP['memory'] =  '253960'  limit 200";

        SqlNode sqlNode = planner.parse(sql);
        System.out.println(sqlNode.toString());
        sql = "select _MAP['memory'],COUNT(_MAP['event.dataset']),count(_MAP['IP']) from \"ES\".\"kibana_sample_data_logs\" " +
                "WHERE _MAP['memory'] IS NOT NULL GROUP BY _MAP['memory'] limit 200";
        // 执行查询
        statement = connection.createStatement();

        try {
            resultSet = statement.executeQuery(sql);
            // 打印结果
            while (resultSet.next()) {
                System.out.println(resultSet.getObject(1) + ", " + resultSet.getObject(2)
                        + ", " + resultSet.getObject(3));
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            statement.close();
            connection.close();
            // 客户端关了才会关闭程序
            restClient.close();
        }


    }


}
