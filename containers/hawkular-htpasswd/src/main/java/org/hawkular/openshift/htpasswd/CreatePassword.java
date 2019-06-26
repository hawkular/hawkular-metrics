/*
 * Copyright 2014-2019 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.openshift.htpasswd;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.hawkular.openshift.auth.PasswordManager;

public class CreatePassword {

    private static final String SHA256_ALGORITHM = "pbkdf2-sha256";
    private static final String SHA512_ALGORITHM = "pbkdf2-sha512";

    private String algorithm;
    private String fileName;
    private String password;
    private String username;
    private boolean overwrite;

    private PasswordManager passwordManager = new PasswordManager();

    public CreatePassword() {
        password = System.getProperty("htpasswd.password", "");
        username = System.getProperty("htpasswd.username", "");
        algorithm = System.getProperty("htpasswd.algorithm", SHA256_ALGORITHM);
        fileName = System.getProperty("htpasswd.file", "");
        overwrite = Boolean.getBoolean("htpasswd.overwrite");

    }

    private void validateArguments() {
        if (password.isEmpty()) {
            throw new RuntimeException("htpasswd.password cannot be empty");
        }

        if (username.isEmpty()) {
            throw new RuntimeException("htpasswd.username cannot be empty");
        }

        if (!algorithm.equals(SHA256_ALGORITHM) && !algorithm.equals(SHA512_ALGORITHM)) {
            throw new RuntimeException("Invalid algorithm, permitted values: [" + SHA256_ALGORITHM + "," + SHA512_ALGORITHM + "]");
        }
    }

    private void run() throws Exception {

        String output = null;
        OutputStream outputStream;

        // First validate arguments.
        this.validateArguments();

        if (algorithm.equals(SHA256_ALGORITHM)) {
            output = passwordManager.createPBDKF2SHA256Password(password);
        } else if (algorithm.equals(SHA512_ALGORITHM)) {
            output = passwordManager.createPBDKF2SHA512Password(password);
        }

        if (!fileName.isEmpty()) {
            outputStream = new FileOutputStream(this.fileName, !overwrite);
        } else {
            outputStream = System.out;
        }

        if (output != null) {
            try {
                outputStream.write(username.getBytes());
                outputStream.write(":".getBytes());
                outputStream.write(output.getBytes());
                outputStream.write("\n".getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                outputStream.close();
            }
        } else {
            throw new RuntimeException("Invalid output from algorithm, this should not happens, could be a bug");
        }
    }

    public static void main(String[] args) {
        CreatePassword passwordCreator = new CreatePassword();
        try {
            passwordCreator.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
