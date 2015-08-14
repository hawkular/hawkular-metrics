package org.hawkular.metrics.api.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by miburman on 8/13/15.
 */
@WebServlet(urlPatterns = "/chain/*")
public class RoutingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {


        System.out.println("Servlet ==> Requested PATH: " + req.getPathInfo());

        req.getRequestDispatcher("/tenantHandler").include(req, resp);

        System.out.println("Servlet ==> We got back.. use forward if you don't want that");

//        super.doGet(req, resp);
    }
}
