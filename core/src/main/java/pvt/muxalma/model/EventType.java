package pvt.muxalma.model;

public enum EventType {
    OPEN,   // payload: "host:port"
    DATA,   // payload: raw bytes
    CLOSE,  // payload: ignored
    ABORT   // protocol or network error, payload: error message, may have any seq id
}