package cis5550.webserver;

import java.util.HashMap;

public class SessionImpl implements Session {

    private String id;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private boolean isValid;
    private HashMap<String, Object> attributes;

    public SessionImpl(String id) {
        this.id = id;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = System.currentTimeMillis();
        this.maxActiveInterval = 300;
        this.isValid = true;
        this.attributes = new HashMap<>();
    }

    @Override
    public String id() {
        checkExpired();
        checkInvalidate();
        return id;
    }

    @Override
    public long creationTime() {
        checkExpired();
        checkInvalidate();
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        checkExpired();
        checkInvalidate();
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        checkExpired();
        checkInvalidate();
        if (seconds <= 0) {
            throw new IllegalArgumentException("Invalid maxActiveInterval");
        }
        maxActiveInterval = seconds;
    }

    @Override
    public void invalidate() {
        isValid = false;
        attributes.clear();
    }

    @Override
    public Object attribute(String name) {
        checkExpired();
        checkInvalidate();
        lastAccessedTime = System.currentTimeMillis();
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        checkExpired();
        checkInvalidate();
        lastAccessedTime = System.currentTimeMillis();
        attributes.put(name, value);
    }

    public void setLastAccessedTime(long lastAccessedTime) {
        checkExpired();
        checkInvalidate();
        this.lastAccessedTime = lastAccessedTime;
    }

    public void checkInvalidate() {
        if (!isValid) {
            throw new IllegalStateException("Session is invalidated (expired)");
        }
    }

    public boolean checkExpired() {
        if (!isValid) {
            return true;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastAccessedTime > maxActiveInterval * 1000L) {
            invalidate();
            return true;
        }
        return false;
    }
}
