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
@WebServlet(urlPatterns = "/hawkular/metrics/*")
public class MetricsServlet extends HttpServlet {

    private static final String PATH_START = "/hawkular/metrics/";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Always use getRequestURI, it's not decoded. Does not have queryparams (stuff after ?)
        System.out.println("RequestURI: " + req.getRequestURI());
        // To get QueryParams, use the following
        System.out.println("QueryString: " + req.getQueryString());

        req.getRequestDispatcher("/tenantHandler").include(req, resp);

        System.out.println("Servlet ==> We got back.. use forward if you don't want that");

//        super.doGet(req, resp);
    }

    private String[] getQueryParts(HttpServletRequest req) {
        String queryURI = req.getRequestURI().substring(req.getRequestURI().indexOf(PATH_START), PATH_START.length());
        return queryURI.split("/");
    }

}
