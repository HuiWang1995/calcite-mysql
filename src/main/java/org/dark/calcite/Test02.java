package org.dark.calcite;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * test connect mysql by calcite
 *
 * to run this test normally please run init2.sql on your mysql server first
 *
 * schema: test & test2
 * table: test.student & test2.student2
 */
public class Test02 {

    // two schema query for mysql
    public static void main(String[] args) throws Exception {
        // check driver exist
        Class.forName("org.apache.calcite.jdbc.Driver");
        Class.forName("com.mysql.jdbc.Driver");

        // the properties for calcite connection
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("remarks","true");
        // SqlParserImpl can analysis sql dialect for sql parse
        info.setProperty("parserFactory","org.apache.calcite.sql.parser.impl.SqlParserImpl#FACTORY");

        // create calcite connection and schema
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection.getProperties());
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // code for mysql datasource
        MysqlDataSource dataSource = new MysqlDataSource();
        // please change host and port maybe like "jdbc:mysql://127.0.0.1:3306/test"
        dataSource.setUrl("jdbc:mysql://aaa.club/test");
        dataSource.setUser("root");
        dataSource.setPassword("123456");
        // mysql schema, the sub schema for rootSchema, "test" is a schema in mysql
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);


        // code for mysql datasource2
        MysqlDataSource dataSource2 = new MysqlDataSource();
        dataSource2.setUrl("jdbc:mysql://aaa.club/test2");
        dataSource2.setUser("root");
        dataSource2.setPassword("123456");
        // mysql schema, the sub schema for rootSchema, "test" is a schema in mysql
        Schema schema2 = JdbcSchema.create(rootSchema, "test2", dataSource2, null, "test2");
        rootSchema.add("test2", schema2);


        // run sql query
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test.student " +
                "left join test2.student2 on test.student.pk_id = test2.student2.pk_id");
        while (resultSet.next()) {
            System.out.println(resultSet.getObject(1) + "__" + resultSet.getObject(2));
        }

        statement.close();
        connection.close();


    }




}
