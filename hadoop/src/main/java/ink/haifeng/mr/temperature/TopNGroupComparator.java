package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroupComparator extends WritableComparator {

    public TopNGroupComparator() {
        super(TempInfo.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TempInfo k1 = (TempInfo) a;
        TempInfo k2 = (TempInfo) b;
        return Integer.compare(k1.getDay() / 100, k2.getDay() / 100);
    }
}
