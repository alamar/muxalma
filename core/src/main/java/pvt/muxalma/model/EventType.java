package pvt.muxalma.model;

public enum EventType {
    OPEN,   // payload: "host:port"
    DATA,   // payload: raw bytes
    CLOSE   // payload: ignored
}