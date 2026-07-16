package pvt.muxalma.mdns

import pvt.muxalma.fanservice.Lifecycle
import pvt.muxalma.fanservice.Muxalma
import pvt.muxalma.masculine.HttpProxyClient
import pvt.muxalma.model.NetworkEvent
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import javax.jmdns.JmDNS
import javax.jmdns.ServiceInfo

const val relayPort = 21285

fun main() {
    val jmdns = JmDNS.create(InetAddress.getLocalHost())
    val serviceInfo = ServiceInfo.create("_muxalma._tcp.local.", "prince", relayPort, "muxalma")
    jmdns.registerService(serviceInfo)

    ServerSocket(relayPort).use { serverSocket ->
        println("========================================================")
        println("HTTP Filtering Proxy relay is listening on port ${relayPort}...")
        println("========================================================")

        val lifecycle = Lifecycle()

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(Thread(Runnable {
            println("Shutting down relay...")
            lifecycle.stop()
            println("Relay stopped")
        }))

        while (true) {

            serverSocket.accept().use { socket ->
                ObjectInputStream(socket.getInputStream()).use { ois ->
                    ObjectOutputStream(socket.getOutputStream()).use { oos ->
                        val connLifecycle = lifecycle.newLeaf()
                        println("Accepted connection from " + socket.inetAddress)

                        val input = Muxalma.male(HttpProxyClient { outEvent ->
                            synchronized(connLifecycle) {
                                oos.writeObject(SerializableEvent(outEvent))
                                oos.flush()
                            }
                        }, connLifecycle)

                        try {
                            do {
                                val inEvent: NetworkEvent = ois.readObject() as SerializableEvent
                                input.accept(inEvent)
                            } while (true)
                        } catch (ex: Exception) {
                            println("Connection to ${socket.inetAddress} ended: " + ex.message)
                            connLifecycle.stop();
                        }
                    }
                }
            }
        }
    }
}