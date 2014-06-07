package org.rhq.metrics.restServlet;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiClass;
import com.wordnik.swagger.annotations.ApiProperty;

/**
 * Encapsulate a simple string value
 * @author Heiko W. Rupp
 */
@ApiClass("Encapsulates a simple string value. In XML this is represented as <value value=\"...\"/>")
@XmlRootElement(name =  "value")
public class StringValue {

    String value;

    public StringValue() {
    }

    public StringValue(String value) {
        this.value = value;
    }

    @XmlAttribute
    @ApiProperty("The actual value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
