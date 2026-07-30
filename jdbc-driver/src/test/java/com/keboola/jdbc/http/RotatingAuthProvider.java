package com.keboola.jdbc.http;

import com.keboola.jdbc.auth.AuthMode;
import com.keboola.jdbc.auth.AuthProvider;
import com.keboola.jdbc.auth.StorageTokenAuthProvider;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test provider that returns a different token on every call, standing in for a credential
 * that renews itself. Lets a test prove the clients resolve auth headers per request attempt
 * instead of capturing them once.
 */
final class RotatingAuthProvider implements AuthProvider {

    private final AtomicInteger calls = new AtomicInteger();

    /** Token handed out on the n-th (1-based) call to {@link #authHeaders()}. */
    static String tokenForCall(int call) {
        return "token-" + call;
    }

    /** Number of times headers have been resolved. */
    int callCount() {
        return calls.get();
    }

    @Override
    public Map<String, String> authHeaders() {
        return Collections.singletonMap(
                StorageTokenAuthProvider.HEADER_STORAGE_TOKEN,
                tokenForCall(calls.incrementAndGet()));
    }

    @Override
    public AuthMode mode() {
        return AuthMode.STORAGE_TOKEN;
    }
}
