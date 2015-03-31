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

package org.hawkular.metrics.api.jaxrs.service.processor;

import java.io.Writer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;
import javax.ws.rs.Path;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

public class ServiceInterfaceProcessor extends AbstractProcessor {
    static {
        System.setProperty("org.freemarker.loggerLibrary", "NONE");
    }

    private final Configuration ftlConfig;

    public ServiceInterfaceProcessor() {
        ftlConfig = new Configuration(Configuration.VERSION_2_3_22);
        ftlConfig.setClassForTemplateLoading(this.getClass(), "/");
        ftlConfig.setDefaultEncoding("UTF-8");
        ftlConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        Set<String> set = new HashSet<>();
        set.add(Path.class.getCanonicalName());
        return Collections.unmodifiableSet(set);
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(Path.class);
        for (Element element : elements) {
            if (element.getKind() == ElementKind.INTERFACE) {
                try {
                    if (!processElement((TypeElement) element)) {
                        return true;
                    }
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Kind.ERROR, getMessage(e));
                    return true;
                }
            }
        }
        return true;
    }

    private boolean processElement(TypeElement element) throws Exception {
        ServiceModel model = new ServiceModel(element);
        Filer filer = processingEnv.getFiler();
        JavaFileObject sourceFile = filer.createSourceFile(model.getServiceClass() + "Base");
        Template template = ftlConfig.getTemplate("ServiceBasejava.java.ftl");
        try (Writer out = sourceFile.openWriter()) {
            template.process(model, out);
        }
        processingEnv.getMessager().printMessage(Kind.NOTE, "Generated " + sourceFile.toUri().getPath());
        return true;
    }

    private String getMessage(Exception e) {
        return "Cannot process JAX-RS service interface: " + e.getMessage();
    }
}