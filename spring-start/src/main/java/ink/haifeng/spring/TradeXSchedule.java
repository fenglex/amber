package ink.haifeng.spring;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author: haifeng
 * @update: 2024/3/19 10:40
 * @version: 1.0
 */
@Component
public class TradeXSchedule {

    private int curTime=100;
    private void updateQueryBalanceState(){

    }

    private List<FundProduct> getFundProduct() {
        return null;
    }


    @Scheduled(cron = "*/10 * * * * ?")
    public void queryBalance() {
        if (curTime >= 925 && curTime <= 1500) {
            List<FundProduct> fundProductList = getFundProduct();
            fundProductList.forEach(fundProduct -> {
                System.out.println("查询账户余额");
            });
            updateQueryBalanceState();
        }
        //
    }

    @Scheduled(cron = "5 * * * * ?")
    public void queryBalanceBod() {
        if (curTime >= 720 && curTime <= 923) {
            List<FundProduct> fundProductList = getFundProduct();
            fundProductList.forEach(fundProduct -> {
                System.out.println("查询账户余额");
            });
            updateQueryBalanceState();
        }
    }

    @Scheduled(cron = "5 * * * * ?")
    public void queryBalanceEod() {
        if (curTime >= 1505 && curTime < 2200) {
            List<FundProduct> fundProductList = getFundProduct();
            fundProductList.forEach(fundProduct -> {
                System.out.println("查询账户余额");
            });
            updateQueryBalanceState();
        }
    }
}
