package org.rhq.metrics.restServlet.influx.query;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class ParseTreeWalkerProducer {

    @Produces
    @ApplicationScoped
    @InfluxQueryParseTreeWalker
    public ParseTreeWalker parseTreeWalker() {
        return ParseTreeWalker.DEFAULT;
    }

}
