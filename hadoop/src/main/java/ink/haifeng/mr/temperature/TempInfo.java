package ink.haifeng.mr.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TempInfo implements WritableComparable<TempInfo> {
    private int day;
    private int temperature;

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int compareTo(TempInfo o) {
        return Integer.compare(this.day, o.day);
    }

    public void write(DataOutput out) throws IOException {
        out.write(this.day);
        out.write(this.temperature);
    }

    public void readFields(DataInput in) throws IOException {
        this.day = in.readInt();
        this.temperature = in.readInt();
    }
}
