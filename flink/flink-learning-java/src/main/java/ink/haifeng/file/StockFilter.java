package ink.haifeng.file;

import cn.hutool.core.io.FileUtil;
import ink.haifeng.StockData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author haifeng
 */
public class StockFilter {
    public static void main(String[] args) throws IOException {
        String input = "/Users/haifeng/Documents/stock-20220322.csv";
        String output = "/Users/haifeng/Documents/stock-filter-20220322.csv";
        FileUtil.del(output);
        BufferedReader reader = FileUtil.getReader(input, StandardCharsets.UTF_8);
        BufferedWriter writer = FileUtil.getWriter(output, StandardCharsets.UTF_8, true);
        String line;
        int row = 0;
        while ((line = reader.readLine()) != null) {
            StockData data = new StockData(line);
            String stockCode = data.getStockCode();
            int minute = data.getRealtime() / 100000;
            if (minute >= 800 && minute <= 1500) {
                if ((stockCode.startsWith("00")
                        || stockCode.startsWith("30")
                        || stockCode.startsWith("60")
                        || stockCode.startsWith("68"))) {
                    writer.write(line + "\n");
                }
            }
            row += 1;
            if (row % 500000 == 0) {
                System.out.println("row:" + row);
            }
        }
        System.out.println("row:" + row);
    }

}
