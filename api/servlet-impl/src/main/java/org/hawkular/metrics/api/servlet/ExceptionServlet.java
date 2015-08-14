package org.hawkular.metrics.api.servlet;

import org.hawkular.metrics.core.api.TenantAlreadyExistsException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by miburman on 8/14/15.
 */
@WebServlet(urlPatterns = "/ExceptionHandler")
public class ExceptionServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Throwable t = (Throwable) req.getAttribute(RequestDispatcher.ERROR_EXCEPTION);

        if(t instanceof TenantAlreadyExistsException) {
            resp.setStatus(HttpServletResponse.SC_CONFLICT);
        }
    }
}
