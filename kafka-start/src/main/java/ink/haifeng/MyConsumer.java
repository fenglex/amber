package ink.haifeng;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

/**
 * MyConsumer
 *
 * @author haifeng
 * @version 2023/1/4 20:53
 */
public class MyConsumer {
    public static void main(String[] args) {

    }

    @Test
    public void consumer() {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node01:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String lingerMsConfig = ProducerConfig.LINGER_MS_CONFIG;
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //一个运行的consumer ，那么自己会维护自己消费进度
        //一旦你自动提交，但是是异步的
        //1，还没到时间，挂了，没提交，重起一个consuemr，参照offset的时候，会重复消费
        //2，一个批次的数据还没写数据库成功，但是这个批次的offset背异步提交了，挂了，重起一个consuemr，参照offset的时候，会丢失消费
//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"15000");//5秒
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,""); // POLL 拉取数据，弹性，按需，拉取多少？

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Collections.singletonList("topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsRevoked:");
                Iterator<TopicPartition> iter = partitions.iterator();
                while (iter.hasNext()) {
                    System.out.println(iter.next().partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsAssigned:");
                Iterator<TopicPartition> iter = partitions.iterator();
                while (iter.hasNext()) {
                    System.out.println(iter.next().partition());
                }
            }
        });
        /**
         * 以下代码是你再未来开发的时候，向通过自定时间点的方式，自定义消费数据位置
         *
         * 其实本质，核心知识是seek方法
         *
         * 举一反三：
         * 1，通过时间换算出offset，再通过seek来自定义偏移
         * 2，如果你自己维护offset持久化~！！！通过seek完成
         *
         */
        Map<TopicPartition, Long> tts = new HashMap<>();
        //通过consumer取回自己分配的分区 as
        Set<TopicPartition> as = consumer.assignment();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
            if (!records.isEmpty()) {
                //以下代码的优化很重要
                System.out.println("-----------"+records.count()+"-------------");
                Set<TopicPartition> partitions = records.partitions(); //每次poll的时候是取多个分区的数据
                //且每个分区内的数据是有序的
                /**
                 * 如果手动提交offset
                 * 1，按消息进度同步提交
                 * 2，按分区粒度同步提交
                 * 3，按当前poll的批次同步提交
                 *
                 * 思考：如果在多个线程下
                 * 1，以上1，3的方式不用多线程
                 * 2，以上2的方式最容易想到多线程方式处理，有没有问题？
                 */
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    //在一个微批里，按分区获取poll回来的数据
                    //线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> piter = pRecords.iterator();
                    while(piter.hasNext()){
                        ConsumerRecord<String, String> next = piter.next();
                        int par = next.partition();
                        long offset = next.offset();
                        String key = next.key();
                        String value = next.value();
                        long timestamp = next.timestamp();
                        System.out.println("key: "+ key+" val: "+ value+ " partition: "+par + " offset: "+ offset+"time:: "+ timestamp);
                        TopicPartition sp = new TopicPartition("msb-items", par);
                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
                        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(sp,om);
                        consumer.commitSync(map);//这个是最安全的，每条记录级的更新，第一点
                        //单线程，多线程，都可以
                    }
                    long poff = pRecords.get(pRecords.size() - 1).offset();//获取分区内最后一条消息的offset
                    OffsetAndMetadata pom = new OffsetAndMetadata(poff);
                    HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(partition,pom);
                    consumer.commitSync( map );//这个是第二种，分区粒度提交offset
                }
                consumer.commitSync();//这个就是按poll的批次提交offset，第3点
            }
        }
    }
}
