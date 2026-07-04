package com.openexchange.oms.api.auth;

import java.util.Map;
import java.util.TreeMap;

/** Case-insensitive header map for provider tests. */
final class TestHeaders {

    private TestHeaders() {}

    static AuthenticationProvider.Headers of(String... nameValuePairs) {
        Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            map.put(nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return map::get;
    }
}
