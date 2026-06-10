package ink.haifeng.quotation.handler;

import cn.hutool.core.date.DateUtil;
import ink.haifeng.quotation.common.Constants;
import ink.haifeng.quotation.model.dto.StockData;
import ink.haifeng.quotation.model.dto.StockDataWithPre;
import ink.haifeng.quotation.model.entity.StockDaily;
import ink.haifeng.quotation.sink.StockDailySink;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/19 19:00:52
 */
public class StockDailyNoOutputHandler implements NoOutputHandler<SingleOutputStreamOperator<StockDataWithPre>> {
    @Override
    public void handler(SingleOutputStreamOperator<StockDataWithPre> stream, Properties properties) {
        SingleOutputStreamOperator<StockData> dailyStream =
                stream.map(StockDataWithPre::getCurrent).keyBy(StockData::getTradeDay).process(new KeyedProcessFunction<Integer, StockData, StockData>() {
                    private MapState<String, StockData> mapState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, StockData> stringStockDataMapStateDescriptor =
                                new MapStateDescriptor<>(
                                "current_day_last", Types.STRING(), Types.POJO(StockData.class));
                        mapState = this.getRuntimeContext().getMapState(stringStockDataMapStateDescriptor);
                    }

                    @Override
                    public void processElement(StockData value,
                                               KeyedProcessFunction<Integer, StockData, StockData>.Context ctx,
                                               Collector<StockData> out) throws Exception {
                        String runDay = properties.getProperty(Constants.RUN_DAY, DateUtil.format(new Date(),
                                "yyyyMMdd"));
                        int currentDay = Integer.parseInt(runDay);
                        if (value.getTradeDay() == currentDay) {
                            mapState.put(value.getStockCode(), value);
                            if (value.minute() == Constants.MINUTE_15_00) {
                                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000L);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        KeyedProcessFunction<Integer, StockData, StockData>.OnTimerContext ctx,
                                        Collector<StockData> out) throws Exception {
                        String runDay = properties.getProperty(Constants.RUN_DAY, DateUtil.format(new Date(),
                                "yyyyMMdd"));
                        int currentDay = Integer.parseInt(runDay);
                        if (!mapState.isEmpty()) {
                            for (StockData value : mapState.values()) {
                                if (value.getTradeDay() == currentDay) {
                                    out.collect(value);
                                }
                            }
                            mapState.clear();
                        }
                    }
                });
        dailyStream.print("stock_eod");
        StockDailySink stockDailySink = new StockDailySink(properties);
        dailyStream.map(StockDaily::new).addSink(stockDailySink.sink());
    }
}
