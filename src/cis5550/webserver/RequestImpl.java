package cis5550.webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String, String> headers;
    Map<String, String> queryParams;
    Map<String, String> params;
    byte[] bodyRaw;
    Server server;
    Session session;
    ResponseImpl response;

    RequestImpl(String methodArg,
                String urlArg,
                String protocolArg,
                Map<String, String> headersArg,
                Map<String, String> queryParamsArg,
                Map<String, String> paramsArg,
                InetSocketAddress remoteAddrArg,
                byte[] bodyRawArg,
                Server serverArg,
                Session sessionArg,
               ResponseImpl response) {
        this.method = methodArg;
        this.url = urlArg;
        this.remoteAddr = remoteAddrArg;
        this.protocol = protocolArg;
        this.headers = headersArg;
        this.queryParams = queryParamsArg;
        this.params = paramsArg;
        this.bodyRaw = bodyRawArg;
        this.server = serverArg;
        this.session = sessionArg;
        this.response = response;
    }

    public String requestMethod() {
        return method;
    }

    public void setParams(Map<String, String> paramsArg) {
        params = paramsArg;
    }

    public int port() {
        return remoteAddr.getPort();
    }

    public String url() {
        return url;
    }

    public String protocol() {
        return protocol;
    }

    public String contentType() {
        return headers.get("content-type");
    }

    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }

    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }

    public byte[] bodyAsBytes() {
        return bodyRaw;
    }

    public int contentLength() {
        return bodyRaw.length;
    }

    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }

    public Set<String> headers() {
        return headers.keySet();
    }

    public String queryParams(String param) {
        return queryParams.get(param);
    }

    public Set<String> queryParams() {
        return queryParams.keySet();
    }

    public String params(String param) {
        return params.get(param);
    }

    @Override
    public Session session() {
        if (session != null) {
            return session;
        }

        // Create a new session
        String sessionId = setSessionID();
        session = new SessionImpl(sessionId);
        server.sessions.put(sessionId, session);

        // Set the cookie header
        StringBuilder cookie = new StringBuilder();
        cookie.append("SessionID=").append(sessionId);

        // Set the httpOnly flag
        cookie.append("; HttpOnly");

        // Set strict sameSite policy
        cookie.append("; SameSite=Strict");

        // Set secure if https
        if (server.isSecure) {
            cookie.append("; Secure");
        }

        response.header("Set-Cookie", cookie.toString());

        return session;
    }

    public Map<String, String> params() {
        return params;
    }

    private String setSessionID() {
        String set = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz"
                + "?!";

        StringBuilder id = new StringBuilder(20);
        for (int i = 0; i < 20; i++) {
            int index = (int) (set.length() * Math.random());
            id.append(set.charAt(index));
        }

        return id.toString();
    }
}
