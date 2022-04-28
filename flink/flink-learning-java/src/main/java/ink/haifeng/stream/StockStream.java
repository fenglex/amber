package ink.haifeng.stream;

import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.sink.StockAggregate;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * 分多步
 * 1、生成实时数据，写入redis（实际15秒数据）
 * 2、生成个股分钟级别数据
 *
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/18 10:44:59
 */
public class StockStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("/Users/haifeng/workspace/Projects/amber" +
                "/flink/flink-learning-java/data/output.csv");
        SingleOutputStreamOperator<StockData> filterStream =
                source.map(StockData::new).returns(StockData.class).filter(e -> e.getState() != -1)
                        .filter(e -> e.minute() == 925
                                || (e.minute() >= 930 && e.minute() <= 1129)
                                || (e.minute() >= 1300 && e.minute() <= 1457)
                                || e.minute() == 1500)
                        .filter(e -> e.getPrice().compareTo(new BigDecimal(0)) != 0);
        //RedisSink redisSink = new RedisSink("haifeng.ink", 6379, "c99ecff", 1);
        // 生成个股实时数据
//        filterStream.assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(7))
//                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()))
//                .keyBy(StockData::getStockCode)
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                .reduce((v1, v2) -> v1.getTimestamp() > v2.getTimestamp() ? v1 : v2)
//                .map(StockQuotation::new)
//                .addSink((SinkFunction) redisSink);

        // 生成个股分钟数据(需要处理日期切换)
        filterStream
                //.filter(e->e.getStockCode().startsWith("600519"))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(7))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()))
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).allowedLateness(Time.seconds(7))
                .aggregate(new StockAggregate()).map(e -> {
                    StockData current = e.getCurrent();
                    StockData lastMinute = e.getLastMinute();
                    if (lastMinute != null) {
                        return String.format("%s\t%s\t%s\t%s", current.getStockCode(), current.minute(),
                                lastMinute.getStockCode(),
                                lastMinute.minute());
                    } else {
                        return e.getCurrent().toString();
                    }
                }).print("分钟数据测试");
        // 计算每日的交易额交易量需要前一分钟数据

        //ba25435b513cd8aec608869f6a049f5a  quotation-20220418.data

        env.execute("Quotation");

    }
}
