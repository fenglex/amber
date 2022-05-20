package ink.haifeng.quotation.handler;

import ink.haifeng.quotation.model.dto.ProductInfo;
import ink.haifeng.quotation.model.dto.ProductMinuteData;
import ink.haifeng.quotation.model.dto.StockMinuteWithPreData;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/5/20 14:21:08
 */
public class ProductNoOutputHandler implements NoOutputHandler<SingleOutputStreamOperator<StockMinuteWithPreData>> {

    private BroadcastStream<List<ProductInfo>> broadcastStream;

    public ProductNoOutputHandler(BroadcastStream<List<ProductInfo>> broadcastStream) {
        this.broadcastStream = broadcastStream;
    }

    @Override
    public void handler(SingleOutputStreamOperator<StockMinuteWithPreData> stream, Properties properties) {
        SingleOutputStreamOperator<ProductMinuteData> stockConstituentsStream =
                stream.keyBy(e -> e.getCurrent().getStockCode())
                        .connect(broadcastStream)
                        .process(new KeyedBroadcastProcessFunction<String, StockMinuteWithPreData,
                                List<ProductInfo>
                                , ProductMinuteData>() {

                            ListState<ProductInfo> productDimensionDataListState;
                            ValueState<Integer> configDayState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<ProductInfo> stockRelateProductDesc =
                                        new ListStateDescriptor<>("stock_relate_product",
                                        Types.POJO(ProductInfo.class));
                              productDimensionDataListState =
                                        this.getRuntimeContext().getListState(stockRelateProductDesc);
                                ValueStateDescriptor<Integer> configDayStateDesc = new ValueStateDescriptor<>(
                                        "config_day",
                                        Types.INT);
                                configDayState = this.getRuntimeContext().getState(configDayStateDesc);
                            }

                            @Override
                            public void processElement(StockMinuteWithPreData value,
                                                       KeyedBroadcastProcessFunction<String,
                                                               StockMinuteWithPreData, List<ProductInfo>,
                                                               ProductMinuteData>.ReadOnlyContext ctx,
                                                       Collector<ProductMinuteData> out) throws Exception {
                                String currentKey = ctx.getCurrentKey();
                            }

                            @Override
                            public void processBroadcastElement(List<ProductInfo> value,
                                                                KeyedBroadcastProcessFunction<String,
                                                                        StockMinuteWithPreData,
                                                                        List<ProductInfo>,
                                                                        ProductMinuteData>.Context ctx,
                                                                Collector<ProductMinuteData> out) throws Exception {

                            }
                        });
    }
}
