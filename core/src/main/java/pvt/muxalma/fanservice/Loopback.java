package pvt.muxalma.fanservice;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import pvt.muxalma.model.NetworkEvent;

// Нужен для тестов и неразнесённых прокси, чтобы собрать их в правильной последовательности инициализации
public class Loopback implements Consumer<NetworkEvent> {
    private Consumer<NetworkEvent> real;
    private List<NetworkEvent> backlog = new LinkedList<>();

    public synchronized void initialize(Consumer<NetworkEvent> real) {
        this.real = real;
        for (NetworkEvent event : backlog) {
            real.accept(event);
        }
        backlog = null;
    }

    @Override
    public synchronized void accept(NetworkEvent event) {
        if (real == null)
            backlog.add(event);
        else
            real.accept(event);
    }
}
