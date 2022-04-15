package ink.haifeng.data;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.RandomUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/3/28 18:47:20
 */
public class VisitDataGenerate {
    public static void main(String[] args) throws IOException {
        String visitFile = "/Users/haifeng/workspace/Projects/amber/flink/flink-learning-java/data/visit.csv";
        FileUtil.del(visitFile);
        BufferedWriter writer = FileUtil.getWriter(visitFile, StandardCharsets.UTF_8, true);
        int startTm = 1648464521;
        for (int i = 0; i < 10; i++) {
            startTm = startTm + RandomUtil.randomInt(-20, 30);
            String formatDateTime = DateUtil.format(DateUtil.date(startTm * 1000L), "yyyy-MM-dd HH:mm:ss");
            String line = String.format("%s,%s\n", "G5", formatDateTime);
            writer.write(line);
        }
        writer.close();

    }
}
