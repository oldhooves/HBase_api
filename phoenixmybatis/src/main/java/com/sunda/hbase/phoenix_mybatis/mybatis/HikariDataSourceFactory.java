package com.sunda.hbase.phoenix_mybatis.mybatis;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;

/**
 * Created by 老蹄子 on 2018/8/9 下午3:08
 */
public class HikariDataSourceFactory extends UnpooledDataSourceFactory {

    public HikariDataSourceFactory(){
        this.dataSource = new HikariDataSource();
    }
}
