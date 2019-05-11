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

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.commons.codec.digest.Sha2Crypt;

public class PasswordManager {

    private static final String MD5_PREFIX = "$apr1$";
    private static final String SHA_PREFIX = "{SHA}";
    private static final String SHA256_PREFIX = "$5$";
    private static final String SHA512_PREFIX = "$6$";
    private static final String PBKDF2_SHA256_PREFIX = "$pbkdf2-sha256$";
    private static final String PBKDF2_SHA512_PREFIX = "$pbkdf2-sha512$";
    private static final int DEFAULT_ITERATIONS_PBKDF2 = 25000;

    public boolean isAuthorized(String storedCredential, String passedPassword) {
        return (storedCredential.startsWith(MD5_PREFIX) && verifyMD5Password(storedCredential, passedPassword))
                || (storedCredential.startsWith(SHA_PREFIX) && verifySHA1Password(storedCredential, passedPassword))
                ||
                (storedCredential.startsWith(SHA256_PREFIX) && verifySHA256Password(storedCredential, passedPassword))
                ||
                (storedCredential.startsWith(SHA512_PREFIX) && verifySHA512Password(storedCredential, passedPassword))
                || (storedCredential.startsWith(PBKDF2_SHA256_PREFIX) &&
                verifyPBDKF2Password(storedCredential, passedPassword))
                || (storedCredential.startsWith(PBKDF2_SHA512_PREFIX) &&
                verifyPBDKF2Password(storedCredential, passedPassword));
    }


    private boolean verifyMD5Password(String storedCredential, String passedPassword) {
        // We send in the password presented by the user and use the stored password as the salt
        // If they match, then the password matches the original non-encrypted stored password
        return Md5Crypt.apr1Crypt(passedPassword, storedCredential).equals(storedCredential);
    }

    private boolean verifySHA1Password(String storedCredential, String passedPassword) {
        //Remove the SHA_PREFIX from the password string
        String storedPassword = storedCredential.substring(SHA_PREFIX.length());

        //Get the SHA digest and encode it in Base64
        byte[] digestedPasswordBytes = DigestUtils.sha1(passedPassword);
        String digestedPassword = Base64.getEncoder().encodeToString(digestedPasswordBytes);

        //Check if the stored password matches the passed one
        return digestedPassword.equals(storedPassword);
    }

    private boolean verifySHA256Password(String storedCredential, String passedPassword) {
        //Obtain the salt from the beginning of the storedPassword
        String salt = storedCredential.substring(0, storedCredential.lastIndexOf("$") + 1);

        String digestedPassword = Sha2Crypt.sha256Crypt(passedPassword.getBytes(), salt);

        //Check if the stored password matches the passed one
        return digestedPassword.equals(storedCredential);
    }

    private boolean verifySHA512Password(String storedCredential, String passedPassword) {
        //Obtain the salt from the beginning of the storedPassword
        String salt = storedCredential.substring(0, storedCredential.lastIndexOf("$") + 1);

        String digestedPassword = Sha2Crypt.sha512Crypt(passedPassword.getBytes(), salt);

        //Check if the stored password matches the passed one
        return digestedPassword.equals(storedCredential);
    }

    private boolean verifyPBDKF2Password(String storedCredential, String passedPassword) {
        String[] creds = storedCredential.split("\\$");
        if (creds.length != 5) {
            // TODO verify ok to throw exception here if using other password types
            throw new RuntimeException("Stored password checksum not valid. Check password store.");
        }
        String pbkdf2Algorithm = "PBKDF2WithHmacSHA256";
        int keySize = Base64.getDecoder().decode(creds[4]).length * 8;
        if (keySize == 256) {
            // default
        } else if (keySize == 512) {
            pbkdf2Algorithm = "PBKDF2WithHmacSHA512";
        } else {
            throw new RuntimeException("Stored password is not a valid size. Check password store.");
        }
        byte[] salt = Base64.getDecoder().decode(creds[3].getBytes());
        String encoded = encodePBDKF2(passedPassword, Integer.parseInt(creds[2]), salt, keySize, pbkdf2Algorithm);

        return encoded.equals(creds[4]);
    }

    private String encodePBDKF2(String rawPassword, int iterations, byte[] salt, int keySize, String pbkdf2Algorithm) {
        KeySpec spec = new PBEKeySpec(rawPassword.toCharArray(), salt, iterations, keySize);

        try {
            byte[] key = SecretKeyFactory.getInstance(pbkdf2Algorithm).generateSecret(spec).getEncoded();
            return Base64.getEncoder().encodeToString(key);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("PBKDF2 algorithm not found", e);
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException("Password could not be encoded", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String createPBDKF2SHA256Password(String password) {
        return createPBDKF2Password(password, DEFAULT_ITERATIONS_PBKDF2, 256);
    }

    public String createPBDKF2SHA512Password(String password) {
        return createPBDKF2Password(password, DEFAULT_ITERATIONS_PBKDF2, 512);
    }

    private String createPBDKF2Password(String password, int iterations, int keySize) {
        String prefix = PBKDF2_SHA256_PREFIX;
        String pbkdf2Algorithm = "PBKDF2WithHmacSHA256";
        if (keySize == 256) {
            // default
        } else if (keySize == 512) {
            prefix = PBKDF2_SHA512_PREFIX;
            pbkdf2Algorithm = "PBKDF2WithHmacSHA512";
        } else {
            throw new RuntimeException("Keysize must be 256 or 512");
        }
        if (iterations < 10000) {
            throw new RuntimeException("Iterations must be above 10000 when specified.");
        }

        byte[] salt = getSalt();
        String saltEncoded = Base64.getEncoder().encodeToString(salt);
        String checksum = encodePBDKF2(password, iterations, salt, keySize, pbkdf2Algorithm);

        return prefix + iterations + "$" + saltEncoded + "$" + checksum;
    }

    private byte[] getSalt() {
        byte[] buffer = new byte[16];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(buffer);
        return buffer;
    }
}
