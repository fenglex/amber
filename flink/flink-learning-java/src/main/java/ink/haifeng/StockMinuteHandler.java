package ink.haifeng;

import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.ds.simple.SimpleDataSource;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/4/15 14:10:50
 */
public class StockMinuteHandler {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("/Users/haifeng/workspace/Projects/amber/flink/" +
                "flink-learning-java/data/output.csv");
        source.map(StockData::new).returns(TypeInformation.of(StockData.class))
                .filter(e -> e.getState() != -1)
                .filter(e -> e.getMinute() == 925
                        || (e.getMinute() >= 930 && e.getMinute() <= 1129)
                        || (e.getMinute() >= 1300 && e.getMinute() <= 1457)
                        || e.getMinute() == 1500)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((stockData, l) -> stockData.getTimestamp()))
                .keyBy(StockData::getStockCode)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .reduce((v1, v2) -> v1.getTimestamp() > v2.getTimestamp() ? v1 : v2)
                //.print("test");
                .addSink(new StockMinuteSink());

        env.execute("minute stock");

    }

    private static class StockMinuteSink extends RichSinkFunction<StockData> {

        private Db db;

        @Override
        public void open(Configuration parameters) {
            String url = "jdbc:mysql://127.0.0.1:3306/db_quotation";
            String password = "123456";
            String user = "root";
            SimpleDataSource dataSource = new SimpleDataSource(url, user, password);
            db = DbUtil.use(dataSource);
        }

        @Override
        public void invoke(StockData value, Context context) throws Exception {
            db.execute("INSERT INTO tb_stock_minute (stock_code,data,minute,realtime) values (?,?,?,?) ",
                    value.getStockCode(), JSON.toJSONString(value), value.getMinute(), value.getRealtime());
        }

        @Override
        public void close() throws Exception {
            System.out.println("close data");
        }
    }
}
