package pvt.muxalma.mdns

import pvt.muxalma.fanservice.Lifecycle
import pvt.muxalma.fanservice.Muxalma
import pvt.muxalma.model.NetworkEvent
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.InetAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import javax.jmdns.JmDNS
import javax.jmdns.ServiceEvent
import javax.jmdns.ServiceListener
import kotlin.system.exitProcess

const val httpProxyPort = 18080

fun main() {
    val jmdns = JmDNS.create(InetAddress.getLocalHost())
    val foundRelay = AtomicBoolean(false)

    val lifecycle = Lifecycle()

    // Добавляем обработчик завершения
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        println("Shutting down relay...")
        lifecycle.stop()
        println("Relay stopped")
    }))

    jmdns.addServiceListener("_muxalma._tcp.local.", object : ServiceListener {
        override fun serviceAdded(event: ServiceEvent?) {
            // No-op
        }

        override fun serviceRemoved(event: ServiceEvent?) {
            // No-op
        }

        override fun serviceResolved(event: ServiceEvent) {
            println("Found relay at " + event.getInfo().getInetAddresses()[0] + ":" + event.getInfo().getPort())
            if (!foundRelay.compareAndSet(false, true))
                return // Only one connection to relay

            Socket(event.getInfo().getInetAddresses()[0], event.getInfo().getPort()).use { socket ->
                ObjectOutputStream(socket.getOutputStream()).use { oos ->
                    ObjectInputStream(socket.getInputStream()).use { ois ->

                        val input = Muxalma.female(httpProxyPort, { outEvent ->
                            synchronized(this) {
                                oos.writeObject(SerializableEvent(outEvent))
                                oos.flush()
                            }
                        }, lifecycle)

                        println("========================================================")
                        println("HTTP Filtering Proxy is running on port ${httpProxyPort}...")
                        println("========================================================")

                        try {
                            do {
                                val inEvent: NetworkEvent = ois.readObject() as SerializableEvent
                                input.accept(inEvent)
                            } while (true)
                        } catch (ex: Exception) {
                            println("Connection to ${socket.inetAddress} ended: " + ex.message)
                            exitProcess(0)
                        }
                    }
                }
            }
        }
    })

    try {
        while (true) {
            Thread.sleep(Long.MAX_VALUE)
        }
    } catch (e: InterruptedException) {
        // Restore interrupted status or handle the shutdown
        Thread.currentThread().interrupt()
        println("Thread was woken up or stopped.")
    }
}