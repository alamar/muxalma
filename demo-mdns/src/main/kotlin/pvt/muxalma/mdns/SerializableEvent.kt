package pvt.muxalma.mdns

import pvt.muxalma.model.EventType
import pvt.muxalma.model.NetworkEvent
import java.io.Serializable
import java.util.UUID

class SerializableEvent private constructor(
    private val connectionId: UUID,
    private val serial: Int,
    private val type: EventType,
    private val payload: ByteArray?) :
    NetworkEvent, Serializable {

    constructor(event: NetworkEvent) : this(event.connectionId, event.serial, event.type, event.payload)

    override fun getConnectionId(): UUID = connectionId
    override fun getSerial(): Int = serial
    override fun getType(): EventType = type
    override fun getPayload(): ByteArray? = payload
}