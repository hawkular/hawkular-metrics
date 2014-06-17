package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiClass;
import com.wordnik.swagger.annotations.ApiProperty;

/**
 * Just a simple representation of a Link
 * @author Heiko W. Rupp
 */
@SuppressWarnings("unused")
@ApiClass("A simple representation of a link.")
@XmlRootElement
public class SimpleLink {
    private String rel;
    private String href;
    private String title;

    public SimpleLink() {
    }

    public SimpleLink(String rel, String href, String title) {
        this.rel = rel;
        this.href = href;
        this.title = title;
    }

    @ApiProperty("Name of the relation")
    public String getRel() {
        return rel;
    }

    @ApiProperty("Href to target entity")
    public String getHref() {
        return href;
    }

    @ApiProperty("Name of the target")
    public String getTitle() {
        return title;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }

    public void setHref(String href) {
        this.href = href;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
