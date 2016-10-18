/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.openshift.auth;

import static org.hawkular.openshift.auth.Utils.endExchange;

import static io.undertow.util.Headers.AUTHORIZATION;
import static io.undertow.util.StatusCodes.FORBIDDEN;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.Md5Crypt;
import org.jboss.logging.Logger;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

/**
 * An authentication strategy which allows requests of users listed in an Apaches's server htpasswd file.
 *
 * @author Thomas Segismont
 */
class BasicAuthenticator implements Authenticator {
    private static final Logger log = Logger.getLogger(BasicAuthenticator.class);

    static final String BASIC_PREFIX = "Basic ";
    private static final String HTPASSWD_FILE_SYSPROP_SUFFIX = ".openshift.htpasswd-file";
    private final File htpasswdFile;

    private static final String MD5_PREFIX = "$apr1$";
    private static final String SHA_PREFIX = "{SHA}";

    private final HttpHandler containerHandler;
    private final Map<String, String> users;

    BasicAuthenticator(HttpHandler containerHandler, String componentName) {
        String htpasswdPath = System.getProperty(componentName + HTPASSWD_FILE_SYSPROP_SUFFIX);
        if (htpasswdPath == null) {
            htpasswdFile = new File(System.getProperty("user.home"), ".htpasswd");
        } else {
            htpasswdFile = new File(htpasswdPath);
        }

        this.containerHandler = containerHandler;
        users = htpasswdFile.canRead() ? Collections.unmodifiableMap(readHtpasswdFile()) : Collections.emptyMap();
    }

    private Map<String, String> readHtpasswdFile() {
        Map<String, String> result = new HashMap<String, String>();
        try (BufferedReader reader = new BufferedReader(new FileReader(htpasswdFile))) {
            reader.lines().forEach(line -> {
                String[] values = line.split(":", 2);
                if (values.length == 2) {
                    result.put(values[0], values[1]);
                }
            });
        } catch (Exception e) {
            log.error("Error trying to setup BasicAuthentication based on an htpasswd file.", e);
        }
        return result;
    }

    @Override
    public void handleRequest(HttpServerExchange serverExchange) throws Exception {
        if (users.isEmpty()) {
            endExchange(serverExchange, FORBIDDEN);
            return;
        }

        String authorizationHeader = serverExchange.getRequestHeaders().getFirst(AUTHORIZATION);
        String usernamePasswordEncoded = authorizationHeader.substring(BASIC_PREFIX.length());
        String usernamePassword = new String(Base64.getDecoder().decode(usernamePasswordEncoded));

        String[] entries = usernamePassword.split(":", 2);
        if (entries.length != 2) {
            endExchange(serverExchange, FORBIDDEN);
            return;
        }

        String username = entries[0];
        String password = entries[1];

        if (users.containsKey(username) && isAuthorized(username, password)) {
            containerHandler.handleRequest(serverExchange);
        } else {
            endExchange(serverExchange, FORBIDDEN);
        }
    }

    private boolean isAuthorized(String username, String password) {
        String storedPassword = users.get(username);
        return (storedPassword.startsWith(MD5_PREFIX) && verifyMD5Password(storedPassword, password))
                || (storedPassword.startsWith(SHA_PREFIX) && verifySHA1Password(storedPassword, password));
    }


    private boolean verifyMD5Password(String storedPassword, String passedPassword) {
        // We send in the password presented by the user and use the stored password as the salt
        // If they match, then the password matches the original non-encrypted stored password
        return Md5Crypt.apr1Crypt(passedPassword, storedPassword).equals(storedPassword);
    }

    private boolean verifySHA1Password(String storedPassword, String passedPassword) {
        //Remove the SHA_PREFIX from the password string
        storedPassword = storedPassword.substring(SHA_PREFIX.length());

        //Get the SHA digest and encode it in Base64
        byte[] digestedPasswordBytes = DigestUtils.sha1(passedPassword);
        String digestedPassword = Base64.getEncoder().encodeToString(digestedPasswordBytes);

        //Check if the stored password matches the passed one
        return digestedPassword.equals(storedPassword);
    }

    @Override
    public void stop() {
    }
}
