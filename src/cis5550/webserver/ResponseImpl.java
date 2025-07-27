package cis5550.webserver;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ResponseImpl implements Response {

    // for headers
    private final Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    // for type
    private String contentType = "text/html";
    // for status
    private int statusCode = 200;
    private String statusPhrase = "OK";
    // for body
    private byte[] body;
    // for socket
    private final Socket sock;
    // for checking if write() has been called
    private boolean committed = false;
    // for checking if halt() has been called
    private boolean halted = false;

    public ResponseImpl(Socket sock) {
        this.sock = sock;
    }

    @Override
    public void body(String body) {
        if (!committed) {
            if (body != null) {
                this.body = body.getBytes();
            } else {
                this.body = null;
            }
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (!committed) {
            this.body = bodyArg;
        }
    }

    @Override
    public void header(String name, String value) {
        // checking if write() has been called
        if (!committed) {
            List<String> values = headers.computeIfAbsent(name, k -> new ArrayList<>());

            // add the value to the list if the value is not in the list
            if (!values.contains(value)) {
                values.add(value);
            }
        }
    }

    @Override
    public void type(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!committed) {
            this.statusCode = statusCode;
            this.statusPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!committed) {
            committed = true;

            PrintWriter out = new PrintWriter(sock.getOutputStream());

            // send the status code and reason phrase
            out.print("HTTP/1.1 " + statusCode + " " + statusPhrase + "\r\n");

            // send the content type
            out.print("Content-Type: " + contentType + "\r\n");

            // send the connection close header
            out.print("Connection: close\r\n");

            // send the headers
            for (String key : headers.keySet()) {
                if (key.equals("Content-Length")) continue;
                List<String> values = headers.get(key);
                for (String value : values) {
                    out.print(key + ": " + value + "\r\n");
                }
            }

            // Send the second CRLF to end the header
            out.print("\r\n");

            out.flush();
            // send the body
            if (b != null) {
                sock.getOutputStream().write(b);
            }

            out.flush();
        } else {
            if (b != null) {
                sock.getOutputStream().write(b);
            }
            sock.getOutputStream().flush();
        }
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (!committed) {
            if (responseCode == 301) {
                status(301, "Moved Permanently");
            } else if (responseCode == 302) {
                status(302, "Found");
            } else if (responseCode == 303) {
                status(303, "See Other");
            } else if (responseCode == 307) {
                status(307, "Temporary Redirect");
            } else if (responseCode == 308) {
                status(308, "Permanent Redirect");
            }

            // add the location header
            header("Location", url);

            // add a body
            body("Redirected to " + url + " with status code " + responseCode);
        }
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        if (!committed) {
            status(statusCode, reasonPhrase);
            this.halted = true;
        }
    }

    public void writeHaltResponse() throws Exception {
        PrintWriter out = new PrintWriter(sock.getOutputStream());
        String responseBody = "Halted with status code " + statusCode + " " + statusPhrase + "\r\n";

        // send the status code and reason phrase
        out.print("HTTP/1.1 " + statusCode + " " + statusPhrase + "\r\n");

        // send the content type
        out.print("Content-Type: " + "text/html" + "\r\n");

        // send the content length
        out.print("Content-Length: " + responseBody.length() + "\r\n");

        // send the headers
        for (String key : headers.keySet()) {
            List<String> values = headers.get(key);
            for (String value : values) {
                out.print(key + ": " + value + "\r\n");
            }
        }

        // Send the second CRLF to end the header
        out.print("\r\n");

        // send the body
        out.print(responseBody);

        out.flush();
    }

    public boolean checkWriteCalled() {
        return committed;
    }

    public byte[] getBody() {
        return body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusPhrase() {
        return statusPhrase;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public boolean checkHalted() {
        return halted;
    }
}
