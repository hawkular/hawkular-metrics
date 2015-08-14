package org.hawkular.metrics.api.filter;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import java.io.IOException;

/**
 * Created by miburman on 8/14/15.
 */
@WebFilter(filterName="metricsFilter", urlPatterns = "/chain/*")
public class MetricsFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        // Before servlets
        System.out.println("Starting filter chain.. ");
        servletRequest.setAttribute("filtered", Boolean.TRUE);

        // To the next filter or servlet
        filterChain.doFilter(servletRequest, servletResponse);

        // After servlet
        System.out.println("Got back from the servlet");
        servletResponse.getWriter().write("I'm a filter addict!");
    }

    @Override
    public void destroy() {

    }
}
