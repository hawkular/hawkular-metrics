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
package org.hawkular.metrics.core.impl.util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Copied from http://tutorials.jenkov.com/java-howto/replace-strings-in-streams-arrays-files.html
 * with fixes to {@link #read(char[], int, int)} and added support for escaping.
 * <p>
 * The following constructs have been added:
 * <ul>
 *     <li>{@code ${p:v}} if token {@code p} is not found in the backing map, the provided value {@code v} is used
 *     instead of keeping the literal "${p}" in the output.</li>
 *     <li>{@code ${p1,p2:v}} if token {@code p1} is not found in the backing map, the expression is replaced by the
 *     value of token {@code p2} or value {@code v} if {@code p2} is not present either.</li>
 * </ul>
 * @author Lukas Krejci
 */
public class TokenReplacingReader extends Reader {

    private PushbackReader pushbackReader = null;
    private Map<String, String> tokens = null;
    private StringBuilder tokenNameBuffer = new StringBuilder();
    private String tokenValue = null;
    private int tokenValueIndex = 0;
    private boolean escaping = false;
    private Deque<String> activeTokens;
    private Map<String, String> resolvedTokens;

    public TokenReplacingReader(String source, Map<String, String> tokens) {
        this.pushbackReader = new PushbackReader(new StringReader(source), 2);
        this.tokens = tokens;
        this.activeTokens = new ArrayDeque<String>();
        this.resolvedTokens = new HashMap<String, String>();
    }

    public TokenReplacingReader(String source, Map<String, String> tokens, Deque<String> activeTokens,
            Map<String, String> resolvedTokens) {
        pushbackReader = new PushbackReader(new StringReader(source));
        this.tokens = tokens;
        this.activeTokens = activeTokens;
        this.resolvedTokens = resolvedTokens;
    }

    public int read(CharBuffer target) throws IOException {
        throw new RuntimeException("Operation Not Supported");
    }

    public int read() throws IOException {
        if (this.tokenValue != null) {
            if (this.tokenValueIndex < this.tokenValue.length()) {
                return this.tokenValue.charAt(this.tokenValueIndex++);
            }
            if (this.tokenValueIndex == this.tokenValue.length()) {
                this.tokenValue = null;
                this.tokenValueIndex = 0;
            }
        }

        int data = this.pushbackReader.read();

        if (escaping) {
            escaping = false;
            return data;
        }

        //only escape the ${ sequence.
        //I.e. \${ is escaped as ${, but \$ is transferred literally
        if (data == '\\') {
            data = pushbackReader.read();
            if (data != '$') {
                pushbackReader.unread(data);
                return '\\';
            } else {
                data = pushbackReader.read();
                pushbackReader.unread(data);
                if (data != '{') {
                    pushbackReader.unread('$');
                    return '\\';
                } else {
                    escaping = true;
                    return '$';
                }
            }
        }

        if (data != '$')
            return data;

        data = this.pushbackReader.read();
        if (data != '{') {
            this.pushbackReader.unread(data);
            return '$';
        }
        this.tokenNameBuffer.delete(0, this.tokenNameBuffer.length());

        String tokenName;

        boolean skipUntilExpressionEnd = false;
        boolean nameIsValue = false;
        boolean cont = true;

        //0 - reading name
        //1 - reading value
        //2 - escape in value
        int state = 0;
        while (cont) {
            data = this.pushbackReader.read();
            switch (state) {
            case 0: //reading name
                switch (data) {
                case ',':
                    if (skipUntilExpressionEnd) {
                        break;
                    }
                    //we've read the name, try if it is available.
                    //if yes, skip everything else until '}' and start outputting the value
                    //if not, try the name specified after the ','
                    tokenName = tokenNameBuffer.toString();
                    if (tokens.containsKey(tokenName)) {
                        skipUntilExpressionEnd = true;
                    } else {
                        //let's try the next token
                        tokenNameBuffer.delete(0, tokenNameBuffer.length());
                    }
                    break;
                case ':':
                    if (skipUntilExpressionEnd) {
                        state = 1; //reading value
                        break;
                    }

                    if (tokenNameBuffer.length() == 0) {
                        //leading : is considered a part of the name
                        tokenNameBuffer.append((char) data);
                    } else {
                        state = 1; //reading value
                        tokenName = tokenNameBuffer.toString();
                        if (tokens.containsKey(tokenName)) {
                            skipUntilExpressionEnd = true;
                        } else {
                            tokenNameBuffer.delete(0, tokenNameBuffer.length());
                            nameIsValue = true;
                        }
                    }
                    break;
                case '}':
                    cont = false;
                    break;
                default:
                    this.tokenNameBuffer.append((char) data);
                }
                break;
            case 1: //reading value
                switch (data) {
                case '\\':
                    data = pushbackReader.read();
                    if (data != '}') {
                        pushbackReader.unread(data);
                        data = '\\';
                    }

                    if (nameIsValue) {
                        tokenNameBuffer.append((char) data);
                    }
                    break;
                case '}':
                    cont = false;
                    break;
                default:
                    if (nameIsValue) {
                        tokenNameBuffer.append((char) data);
                    }
                }
            }
        }

        tokenName = tokenNameBuffer.toString();

        if (nameIsValue) {
            TokenReplacingReader childReader = new TokenReplacingReader(tokenName, tokens, activeTokens,
                resolvedTokens);
            tokenValue = readAll(childReader);
        } else {
            tokenValue = resolveToken(tokenName);
        }

        tokenValueIndex = 0;

        if (!this.tokenValue.isEmpty()) {
            return this.tokenValue.charAt(this.tokenValueIndex++);
        } else {
            return read();
        }
    }

    public int read(char[] cbuf) throws IOException {
        return read(cbuf, 0, cbuf.length);
    }

    public int read(char[] cbuf, int off, int len) throws IOException {
        int i = 0;
        for (; i < len; i++) {
            int nextChar = read();
            if (nextChar == -1) {
                if (i == 0) {
                    i = -1;
                }
                break;
            }
            cbuf[off + i] = (char) nextChar;
        }
        return i;
    }

    public void close() throws IOException {
        this.pushbackReader.close();
    }

    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException("skip() not supported on TokenReplacingReader.");
    }

    public boolean ready() throws IOException {
        return this.pushbackReader.ready();
    }

    public boolean markSupported() {
        return false;
    }

    public void mark(int readAheadLimit) throws IOException {
        throw new IOException("mark() not supported on TokenReplacingReader.");
    }

    public void reset() throws IOException {
        throw new IOException("reset() not supported on TokenReplacingReader.");
    }

    private String readAll(Reader r) throws IOException {
        int c;
        StringBuilder bld = new StringBuilder();
        while((c = r.read()) >= 0) {
            bld.append((char)c);
        }

        return bld.toString();
    }

    private String resolveToken(String tokenName) throws IOException {
        if (resolvedTokens.containsKey(tokenName)) {
            return resolvedTokens.get(tokenName);
        }

        if (activeTokens.contains(tokenName)) {
            throw new IllegalArgumentException("Token '" + tokenName
                    + "' (indirectly) contains reference to itself in its value.");
        }

        activeTokens.push(tokenName);

        String tokenValue = tokens.get(tokenName);

        if (tokenValue != null) {
            if (tokenValue.contains("${")) {
                TokenReplacingReader childReader = new TokenReplacingReader(tokenValue, tokens, activeTokens,
                        resolvedTokens);

                tokenValue = readAll(childReader);
            }
        } else {
            tokenValue = "${" + tokenName + "}";
        }

        resolvedTokens.put(tokenName, tokenValue);

        activeTokens.pop();

        return tokenValue;
    }

}
