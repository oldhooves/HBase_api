package com.sunda.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by 老蹄子 on 2018/8/8 下午7:00
 */
public class RegionObserver extends BaseRegionObserver {

    private byte[] columnFamily = Bytes.toBytes("cf");
    private byte[] countCol = Bytes.toBytes("countCol");
    private byte[] unDeleteCol = Bytes.toBytes("unDeleteCol");
    private RegionCoprocessorEnvironment environment;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        environment = (RegionCoprocessorEnvironment) e;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
                       WALEdit edit, Durability durability) throws IOException {
        if (put.has(columnFamily,countCol)){
            Result rs = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            int oldNum = 0;
            for (Cell cell : rs.rawCells()){
                if (CellUtil.matchingColumn(cell,columnFamily,countCol)){
                    oldNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            List<Cell> cells = put.get(columnFamily,countCol);
            int newNum = 0;
            for (Cell cell : cells){
                if (CellUtil.matchingColumn(cell,columnFamily,countCol)){
                    newNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            put.addColumn(columnFamily,countCol,Bytes.toBytes(String.valueOf(oldNum+newNum)));
        }
    }


    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
                          WALEdit edit, Durability durability) throws IOException {

        List<Cell> cells = delete.getFamilyCellMap().get(columnFamily);
        if (cells == null || cells.size() == 0){
            return;
        }

        boolean deleteFlag = false;
        for (Cell cell : cells){
            byte[] qualifier = CellUtil.cloneQualifier(cell);

            if (Arrays.equals(qualifier,unDeleteCol)){
                throw new IOException("can not delete unDelCol");
            }

            if (Arrays.equals(qualifier,countCol)){
                deleteFlag = true;
            }
        }

        if (deleteFlag){
            delete.addColumn(columnFamily,unDeleteCol);
        }
    }
}
