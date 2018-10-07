package cn.edu.fudan.demo;

import java.nio.ByteBuffer;

/**
 * Created by sherry on 18-9-29.
 */
public class Util {

    public static double[] convertFromByteArray(byte[] byteArray) {
        ByteBuffer bb = ByteBuffer.wrap(byteArray);
        double[] values = new double[byteArray.length / Double.BYTES];
        for(int i = 0; i < values.length; i++) {
            values[i] = bb.getDouble();
        }
        return values;
    }
}
