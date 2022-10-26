package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNSortComparator extends WritableComparator {
    public TopNSortComparator() {
        super(TempInfo.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TempInfo k1 = (TempInfo) a;
        TempInfo k2 = (TempInfo) b;
        int compare = Integer.compare(k1.getDay() / 100, k2.getDay() / 100);
        if (compare == 0) {
            return -Integer.compare(k1.getTemperature(), k2.getTemperature());
        }
        return compare;
    }
}
