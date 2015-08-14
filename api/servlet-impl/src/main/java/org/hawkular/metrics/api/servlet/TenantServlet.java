package org.hawkular.metrics.api.servlet;

import org.hawkular.metrics.core.api.TenantAlreadyExistsException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by miburman on 8/14/15.
 */
@WebServlet(urlPatterns = "/tenantHandler")
public class TenantServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("Servlet ==> TenantHandler: Got the request! " + req.getPathInfo());
        System.out.println("Servlet ==> TenantHandler: Got the request! " + req.getRequestURI());

        if(req.getPathInfo().contains("error")) {
            System.out.println("Throwing exception");
            throw new TenantAlreadyExistsException("We already got you!");
        }

        PrintWriter writer = resp.getWriter();
        resp.setStatus(HttpServletResponse.SC_OK);
        writer.close();
    }
}
