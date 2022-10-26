package ink.haifeng.mr.temperature;

import java.io.Serializable;

/**
 * @author : haifeng
 */
public class TempInfo implements Serializable {
    private String day;
    private int temp;

    public TempInfo(String day, int temp) {
        this.day = day;
        this.temp = temp;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }
}
