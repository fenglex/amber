package ink.haifeng.data;

import cn.hutool.core.io.FileUtil;
import ink.haifeng.quotation.model.dto.StockData;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/28 10:46:09
 */
public class StockDataReader {
    public static void main(String[] args) throws IOException {
        String file = "/Users/haifeng/Documents/quotation-20220418.data";
        BufferedReader reader = FileUtil.getReader(file, StandardCharsets.UTF_8);
        Map<Integer, Integer> minuteCount = new HashMap<Integer, Integer>(500);
        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            count += 1;
            StockData data = new StockData(line);
            Integer aDefault = minuteCount.getOrDefault(data.minute(), 0);
            minuteCount.put(data.minute(), aDefault + 1);
            if (count % 1000000 == 0) {
                System.out.println("line->" + count);
            }
        }
        System.out.println(count);

        Stream<Integer> sorted = minuteCount.keySet().stream().sorted();
        sorted.forEach(k -> {
            System.out.println(k + "->" + minuteCount.get(k));
        });
    }
}
