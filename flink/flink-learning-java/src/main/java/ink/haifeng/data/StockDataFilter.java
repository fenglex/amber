package ink.haifeng.data;

import cn.hutool.core.io.FileUtil;
import ink.haifeng.quotation.model.dto.StockData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/15 13:55:16
 */
public class StockDataFilter {
    public static void main(String[] args) throws IOException {
        String file = "/Users/haifeng/Documents/quotation-20220418.data";
        BufferedReader reader = FileUtil.getReader(file, StandardCharsets.UTF_8);
        String line;
        String output = "/Users/haifeng/workspace/Projects/amber/flink/flink-learning-java/data/original-20220418.csv";
        FileUtil.del(output);
        BufferedWriter writer = FileUtil.getWriter(output, StandardCharsets.UTF_8, true);
        String filter = "002249.SZ,300048.SZ,300198.SZ,300224.SZ,300742.SZ,600166.SH,600478.SH,605100.SH";
        int count = 0;
        while ((line = reader.readLine()) != null) {
            count += 1;
            StockData data = new StockData(line);
            if (Objects.equals(data.getStockCode(), "600519.SH") || filter.contains(data.getStockCode())) {
                //line = line.replaceAll("\001", ",");
                writer.write(line + "\n");
            }
            if (count % 1000000 == 0) {
                System.out.println("line->" + count);
            }
        }
        writer.flush();
        writer.close();
        System.out.println(count);
    }
}
