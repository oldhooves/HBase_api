package com.sunda.hbase.phoenix;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by 老蹄子 on 2018/8/9 下午1:08
 */
public class phoenixTest {

    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        Connection connection = DriverManager.getConnection("jdbc:phoenix:192.168.1.110:2181");

        PreparedStatement preparedStatement = connection.prepareStatement("select * from person");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()){
            System.out.println(resultSet.getString("NAME"));
        }

        preparedStatement.close();
        connection.close();
    }
}
