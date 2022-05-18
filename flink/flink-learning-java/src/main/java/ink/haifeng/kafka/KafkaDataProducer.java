package ink.haifeng.kafka;

import cn.hutool.core.io.FileUtil;
import ink.haifeng.quotation.model.dto.StockData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/22 11:21:49
 */
public class KafkaDataProducer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.8:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 100000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "original_0418_2";
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        BufferedReader reader = FileUtil.getReader("/Users/haifeng/workspace/Projects/amber/flink/flink-learning-java" +
                "/data/original-20220418.csv", StandardCharsets.UTF_8);
        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, line);
            producer.send(msg);
            StockData stockData = new StockData(line);
            System.out.println(stockData);
            if ((stockData.minute() == 1256 || stockData.minute() == 1255 || stockData.minute() == 1254) && stockData.getStockCode().equals("600519.SH")) {
                continue;
            }
            count += 1;
            if (count % 1000 == 0) {
                System.out.println(count);
            }
        }
        System.out.println("send message over." + count);
        producer.flush();
        producer.close();
    }
}
