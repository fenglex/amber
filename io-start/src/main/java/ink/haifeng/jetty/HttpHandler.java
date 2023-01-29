package ink.haifeng.jetty;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;

/**
 * HttpHandler
 *
 * @author haifeng
 * @version 2023/1/29 21:55
 */
public class HttpHandler extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println(req.getQueryString());
        String name = req.getParameter("name");
        System.out.println(name);
        resp.getWriter().write("name:" + new Date());

    }
}