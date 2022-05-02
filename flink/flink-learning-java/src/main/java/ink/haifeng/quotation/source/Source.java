package ink.haifeng.quotation.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface Source {

    public <T> SourceFunction<T> source();
}
