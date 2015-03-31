<#--

    Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
    and other contributors as indicated by the @author tags.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
<#-- @ftlvariable name="" type="org.hawkular.metrics.api.jaxrs.service.processor.ServiceModel" -->
package ${packageName};

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import org.hawkular.metrics.api.jaxrs.ApiError;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/*
 * Generated class file
 */
public abstract class ${serviceName}Base implements ${serviceClass} {

<#list methods as method>
    @Override
    public void ${method.name}(
    <#list method.args as arg>
        ${arg.type} ${arg.name}<#if arg_has_next>,</#if>
    </#list>
    ) {
        ListenableFuture<Response> future = _${method.name}(
        <#list method.filteredArgs as arg>
            ${arg.name}<#if arg_has_next>,</#if>
        </#list>
        );
        addCallback(${method.asyncResponseArg.name}, future);
    }

    protected abstract ListenableFuture<Response> _${method.name}(
    <#list method.filteredArgs as arg>
        ${arg.type} ${arg.name}<#if arg_has_next>,</#if>
    </#list>
    );
    <#if method_has_next>

    </#if>
</#list>


    private static void addCallback(AsyncResponse asyncResponse, ListenableFuture<Response> future) {
        Futures.addCallback(
                future, new FutureCallback<Response>() {
                    @Override
                    public void onSuccess(Response response) {
                        asyncResponse.resume(response);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        String exceptionMsg = Throwables.getRootCause(t).getMessage();
                        String msg = "Failed to perform operation due to an error: " + exceptionMsg;
                        asyncResponse.resume(Response.serverError().entity(new ApiError(msg)).build());
                    }
                }
        );
    }
}
