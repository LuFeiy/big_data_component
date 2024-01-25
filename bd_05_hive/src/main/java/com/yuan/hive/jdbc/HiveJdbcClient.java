package com.yuan.hive.jdbc;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // 替换为您的 Hive 服务器地址
        String hiveUrl = "jdbc:hive2://hadoop102:10000/default";
        String user = "atguigu"; // Hive 用户名，根据实际情况配置
        String password = "000000"; // Hive 密码，根据实际情况配置

        // 创建连接
        Connection con = DriverManager.getConnection(hiveUrl, user, password);

        // 创建语句
        Statement stmt = con.createStatement();

        // 执行查询
        String sql = "SELECT * FROM stu LIMIT 10"; // 替换为您的 SQL 语句
        ResultSet res = stmt.executeQuery(sql);

        // 处理结果
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
            // 依据您的表结构来获取和处理结果
        }

        // 关闭连接
        res.close();
        stmt.close();
        con.close();
    }
}
