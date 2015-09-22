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
package org.hawkular.openshift.auth;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.Md5Crypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mwringe
 */
public class BasicAuthentication {

    public static final String BASIC_PREFIX = "Basic ";

    public static final String HTPASSWD_FILE = System.getProperty("hawkular-metrics.openshift.htpasswd-file", "~/" +
            ".htpasswd");

    public static final String MD5_PREFIX = "$apr1$";
    public static final String SHA_PREFIX = "{SHA}";

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthentication.class);

    private Map<String, String> users = new HashMap<>();

    public BasicAuthentication() throws Exception {
        File passwdFile = new File(HTPASSWD_FILE);
        if (passwdFile.exists() && passwdFile.isFile()) {

            BufferedReader reader = new BufferedReader(new FileReader(passwdFile));

            String line = reader.readLine();
            while (line != null) {
                String[] values = line.split(":", 2);
                if (values.length == 2) {
                    users.put(values[0], values[1]);
                }

                line = reader.readLine();
            }

        }
    }

    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws
            IOException, ServletException {

        if (users == null || users.isEmpty()) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        String basicHeader = request.getHeader(OpenShiftAuthenticationFilter.AUTHORIZATION_HEADER);
        String username, password;

        if (basicHeader != null && basicHeader.startsWith("Basic ")) {
            basicHeader = basicHeader.substring("Basic ".length());

            String usernamePassword = new String(Base64.getDecoder().decode(basicHeader));
            String[] entries = usernamePassword.split(":", 2);

            if (entries.length == 2) {
                username = entries[0];
                password = entries[1];

                if (users.containsKey(username) && isAuthorized(username, password)) {
                    filterChain.doFilter(request, response);
                    return;
                }
            }
        }
        response.sendError(HttpServletResponse.SC_FORBIDDEN);
    }


    private boolean isAuthorized(String username, String password) {
        String storedPassword = users.get(username);

        if (storedPassword != null && storedPassword.startsWith(MD5_PREFIX)) {
            return verifyMD5Password(storedPassword, password);
        } else if (storedPassword !=null && storedPassword.startsWith(SHA_PREFIX)) {
            return verifySHA1Password(storedPassword, password);
        } else {
            return false;
        }
    }


    private boolean verifyMD5Password(String storedPassword, String passedPassword) {
        // We send in the password presented by the user and use the stored password as the salt
        // If they match, then the password matches the original non-encrypted stored password
        String encryptedPassword = Md5Crypt.apr1Crypt(passedPassword, storedPassword);

        if (encryptedPassword.equals(storedPassword)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean verifySHA1Password(String storedPassword, String passedPassword) {
        //Remove the SHA_PREFIX from the password string
        storedPassword = storedPassword.substring(SHA_PREFIX.length());

        //Get the SHA digest and encode it in Base64
        byte[] digestedPasswordBytes = DigestUtils.sha1(passedPassword);
        String digestedPassword = Base64.getEncoder().encodeToString(digestedPasswordBytes);

        //Check if the stored password matches the passed one
        if (digestedPassword.equals(storedPassword)) {
            return true;
        } else {
            return false;
        }
    }

}
