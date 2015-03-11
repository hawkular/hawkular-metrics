/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.api.jaxrs.util;

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Just a simple representation of a Link
 * @author Heiko W. Rupp
 */
@SuppressWarnings("unused")
@ApiModel(value = "A simple representation of a link.")
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

    @ApiModelProperty(value = "Name of the relation")
    public String getRel() {
        return rel;
    }

    @ApiModelProperty(value = "Href to target entity")
    public String getHref() {
        return href;
    }

    @ApiModelProperty(value = "Name of the target")
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
