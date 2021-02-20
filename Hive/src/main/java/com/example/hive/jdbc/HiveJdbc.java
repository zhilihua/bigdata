package com.example.hive.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 需要提前打开hiveserver2
 */
public class HiveJdbc {
    public static void main(String[] args) throws Exception{
        // 加载驱动
        //Class.forName("org.apache.hive.jdbc.HiveDriver");

        // 创建连接
        Connection connection = DriverManager.getConnection("jdbc:hive2://hadoop102:10000", "root", "");

        // 准备sql
        String sql = "select * from default.person";

        // 预编译sql
        PreparedStatement ps = connection.prepareStatement(sql);

        // 执行
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            System.out.println("name:"+resultSet.getString("name")+"---->age:"+
                    resultSet.getInt("age"));
        }
    }
}
