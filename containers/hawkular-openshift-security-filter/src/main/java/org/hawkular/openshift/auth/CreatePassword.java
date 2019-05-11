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

package org.hawkular.openshift.auth;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class CreatePassword {

    private static final String SHA256_ALGORITHM = "pbkdf2-sha256";
    private static final String SHA512_ALGORITHM = "pbkdf2-sha512";

    /**
     * Syntax:
     * <p>
     * java org.hawkular.openshift.auth.CreatePassword [options] password
     * --file Output filename
     * --algorithm pbkdf2-sha256 (default), pbkdf2-sha512
     * <p>
     * java org.hawkular.openshift.auth.CreatePassword --file passwdfile --algorithm pbkdf2-sha256 Pa55word
     * <p>
     * Please do not use this utility if you do not need PBKDF2.  Use any other common library/utility.
     *
     * @param args
     */
    public static void main(String[] args) {
        String syntax = "Syntax: \n" + "java org.hawkular.openshift.auth.CreatePassword [options] password\n" +
                "  --file Output filename\n" + "  --algorithm pbkdf2-sha256 (default), pbkdf2-sha512\n";
        PasswordManager passwordManager = new PasswordManager();
        String filename = null;
        FileOutputStream fileOutputStream = null;
        String algorithm = "";
        String password = null;
        String username = null;

        if (args.length == 0) {
            System.out.print("Error: no input! \n" + syntax);
        } else if (args.length == 1) {
            String output = passwordManager.createPBDKF2SHA256Password(args[0]);
            System.out.println("Your password store entry: \n" + output);
        } else if (args.length == 3 || args.length == 5 || args.length == 7) {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--file":
                        if (i + 1 < args.length) {
                            filename = args[i + 1];
                            try {
                                fileOutputStream = new FileOutputStream(filename, true);
                            } catch (FileNotFoundException e) {
                                System.out.println("Error: Invalid file! \n" + syntax);
                            }
                        }
                        i++;
                        break;
                    case "--algorithm":
                        if (i + 1 < args.length && args[i + 1].equals(SHA256_ALGORITHM)) {
                            algorithm = SHA256_ALGORITHM;
                        } else if (i + 1 < args.length && args[i + 1].equals(SHA512_ALGORITHM)) {
                            algorithm = SHA512_ALGORITHM;
                        } else {
                            System.out.println("Error: Invalid input! \n" + syntax);
                        }
                        i++;
                        break;
                    case "--user":
                        if (i + 1 < args.length) {
                            username = args[i + 1];
                        } else {
                            System.out.println("Error: Invalid input! \n" + syntax);
                        }
                        i++;
                        break;
                    default:
                        password = args[i];
                }
            }
            String output = null;
            if (algorithm.equals(SHA256_ALGORITHM)) {
                output = passwordManager.createPBDKF2SHA256Password(password);
            } else if (algorithm.equals(SHA512_ALGORITHM)) {
                output = passwordManager.createPBDKF2SHA512Password(password);
            }
            if (fileOutputStream != null && output != null && username != null) {
                try {
                    fileOutputStream.write(username.getBytes());
                    fileOutputStream.write(":".getBytes());
                    fileOutputStream.write(output.getBytes());
                    fileOutputStream.write("\n".getBytes());
                } catch (IOException e) {
                    System.out.println("Error: Failed to write to file! \n" + syntax);
                }
            } else {
                System.out.println("Your password store entry: \n" + output);
            }
        } else {
            throw new RuntimeException("Error: Invalid input! \n" + syntax);
        }

    }
}
