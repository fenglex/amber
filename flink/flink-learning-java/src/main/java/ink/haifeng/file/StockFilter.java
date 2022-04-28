package ink.haifeng.file;

import cn.hutool.core.io.FileUtil;
import ink.haifeng.quotation.model.dto.StockData;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author haifeng
 */
public class StockFilter {
    public static void main(String[] args) throws IOException {
        String input = "/Users/haifeng/workspace/Projects/amber/flink/flink-learning-java/data/output.csv";

        BufferedReader reader = FileUtil.getReader(input, StandardCharsets.UTF_8);
        long maxTime = 0;
        String line;
        int row = 0;
        StockData last = null;
        while ((line = reader.readLine()) != null) {
            StockData data = new StockData(line);
            if (last == null) {
                if (data.getTimestamp() > maxTime) {
                    System.out.println(data.getTimestamp() - maxTime);
                    maxTime = data.getTimestamp();
                }
            }
            last = data;
        }
        System.out.println("row:" + row);
    }

}
