package ink.haifeng;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

import java.io.IOException;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/7/26 22:13:16
 */
public class StateApi {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "", new MemoryStateBackend());
        DataSource<Integer> listState = savepoint.readListState("", "", Types.INT);
        listState.print();


    }
}
