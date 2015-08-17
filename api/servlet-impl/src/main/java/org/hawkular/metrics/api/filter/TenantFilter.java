package org.hawkular.metrics.api.filter;

import org.hawkular.metrics.api.servlet.rx.ObservableServlet;
import rx.Observable;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by miburman on 8/17/15.
 */
@WebFilter(urlPatterns = "/hawkular/metrics/*", asyncSupported = true)
public class TenantFilter implements Filter {

    public static final String TENANT_HEADER_NAME = "Hawkular-Tenant";

    private static final String MISSING_TENANT_MSG;

    static {
        MISSING_TENANT_MSG = "Tenant is not specified. Use '"
                + TENANT_HEADER_NAME
                + "' header.";
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain filterChain) throws IOException, ServletException {
        String tenant = ((HttpServletRequest) req).getHeader(TENANT_HEADER_NAME);

        if (tenant != null && !tenant.trim().isEmpty()) {
            req.setAttribute(TENANT_HEADER_NAME, tenant);
            filterChain.doFilter(req, resp);
        } else {
            AsyncContext asyncContext = req.startAsync();
            HttpServletResponse response = (HttpServletResponse) resp;
            ObservableServlet.write(Observable.just(ByteBuffer.wrap(MISSING_TENANT_MSG.getBytes())),
                    resp.getOutputStream())
                    .subscribe(v -> {},
                            t -> {},
                            () -> {
                                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                                asyncContext.complete();
                            });
        }
    }

    @Override
    public void destroy() {

    }
}
