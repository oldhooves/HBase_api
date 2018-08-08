import com.sunda.HBaseConn;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by 老蹄子 on 2018/8/8 下午5:10
 */
public class HBaseConnTest {


    @Test
    public void getConnTest() {
        Connection conn = HBaseConn.getHBaseConn();
        System.out.println(conn.isClosed());
        HBaseConn.closeConn();
        System.out.println(conn.isClosed());
    }

    @Test
    public void getTableTest() {
        try {
            Table table = HBaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
