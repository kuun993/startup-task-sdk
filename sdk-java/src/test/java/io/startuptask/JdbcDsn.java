package io.startuptask;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a DSN of the form {@code scheme://user:password@host:port/database} into a JDBC URL
 * plus credentials. Credentials stay in process memory only: neither exception messages nor
 * {@link #toString()} echo the original DSN or the password.
 */
record JdbcDsn(String jdbcUrl, String user, String password) {

    private static final Pattern DSN = Pattern.compile(
            "^(?<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?<userinfo>[^/]*)@(?<hostport>[^/?]+)"
                    + "(?<path>/[^?]*)?(?:\\?(?<query>.*))?$");

    static JdbcDsn fromEnv(String envName, String jdbcSubprotocol) {
        String raw = System.getenv(envName);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        Matcher matcher = DSN.matcher(raw.trim());
        if (!matcher.matches()) {
            throw new IllegalStateException("cannot parse " + envName
                    + " (expected scheme://user:password@host:port/database)");
        }

        String userInfo = matcher.group("userinfo");
        String user = null;
        String password = null;
        int colon = userInfo.indexOf(':');
        if (colon >= 0) {
            user = decode(userInfo.substring(0, colon));
            password = decode(userInfo.substring(colon + 1));
        } else if (!userInfo.isEmpty()) {
            user = decode(userInfo);
        }

        String hostPort = matcher.group("hostport");
        String host = hostPort;
        String port = "";
        int portColon = hostPort.lastIndexOf(':');
        if (portColon > 0 && hostPort.substring(portColon + 1).chars().allMatch(Character::isDigit)) {
            host = hostPort.substring(0, portColon);
            port = ":" + hostPort.substring(portColon + 1);
        }

        String path = matcher.group("path");
        String query = matcher.group("query");
        StringBuilder url = new StringBuilder("jdbc:").append(jdbcSubprotocol).append("://")
                .append(host).append(port)
                .append(path == null || path.isEmpty() ? "/" : path);
        if (query != null && !query.isEmpty()) {
            url.append('?').append(query);
        }
        return new JdbcDsn(url.toString(), user, password);
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException notPercentEncoded) {
            return value;
        }
    }

    @Override
    public String toString() {
        return "JdbcDsn[jdbcUrl=" + jdbcUrl + ", user=" + user + ", password=***]";
    }
}
