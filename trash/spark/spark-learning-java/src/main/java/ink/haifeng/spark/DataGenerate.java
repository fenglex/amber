package ink.haifeng.spark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.*;
import java.util.Date;

/**
 * @author haifeng
 * @version 1.0
 * @date Created in 2022/8/2 10:19:41
 */
public class DataGenerate {
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager
                .getConnection("jdbc:mysql://172.16.2.8:3306/db_test?useUnicode=true&characterEncoding=utf-8",
                        "root", "123456");

        connection.setAutoCommit(false);
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 1000; j++) {
                String sql = String.format("insert into tb_user%s (id,name,age,create_time) values(?,?,?,?)", i + 1);
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setInt(1, j + 1);
                statement.setString(2, String.format("user_%s_%s", j + 1, i + 1));
                statement.setInt(3, j + 1);
                String format = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
                statement.setString(4, format);
                statement.execute();
            }
            connection.commit();
            System.out.println("end" + i);
        }
    }
}
