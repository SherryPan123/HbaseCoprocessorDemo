package cn.edu.fudan.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sherry on 18-9-27.
 */
public class LoadDataMain {

    private static byte[] generateDataOfRow(int number) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(number * Double.BYTES);
        for (int i = 0; i < number; i++) {
            byteBuffer.putDouble(Math.random() * 100);
        }
        return byteBuffer.array();
    }

    private static void output(double[] values) {
        System.out.println("row:");
        for(int i = 0; i < values.length; i++) {
            System.out.print(" " + values[i]);
        }
        System.out.println();
    }

    private static double getSum(double[] values) {
        double valueSum = 0;
        for(double v : values) {
            valueSum += v;
        }
        return valueSum;
    }

    public static void main(String[] args) throws IOException {

        Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.addResource(new Path("hbase-site.xml"));
        Connection con = ConnectionFactory.createConnection(hBaseConfig);
        Admin admin = con.getAdmin();

        TableName tableName = TableName.valueOf("time_series");
        byte[] family = Bytes.toBytes("family");
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family))
                .build();
        //admin.createTable(desc);

        // generate data
        /*
        int rowCount = 10000;
        List<Put> puts = new ArrayList(rowCount);
        try (Table table = con.getTable(tableName)) {
            for(int row = 1; row <= rowCount; row++) {
                // generate each row
                byte[] values = generateDataOfRow(1000);
                if (row % 1000 == 0) {
                    System.out.println(row);
                    table.put(puts);
                    puts = new ArrayList<>();
                }
                Put put = new Put(values);
                put.addColumn(family, Bytes.toBytes("values"), values);
                puts.add(put);
            }
        }

        System.out.println("Insert 10000000 time series");
        */

        /* confirm data output
        Scan scan = new Scan();
        scan.addFamily(family);
        try (Table table = con.getTable(tableName)) {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                byte[] row = result.getRow();
                output(Util.convertFromByteArray(row));
            }
        }
        */

        Scan scan = new Scan();
        scan.addFamily(family);
        double sum = 0d;
        try (Table table = con.getTable(tableName)) {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                byte[] row = result.getRow();
                sum += getSum(Util.convertFromByteArray(row));
            }
        }
        System.out.println("Total sum is: " + sum);


    }

}
