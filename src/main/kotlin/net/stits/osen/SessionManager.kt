package net.stits.osen

import java.nio.channels.AsynchronousSocketChannel

data class PeerNyms(val readNym: Address, val writeNym: Address)

class SessionManager {
    private val readableSessions = hashMapOf<Address, TCPReadableSession>()
    private val writableSessions = hashMapOf<Address, TCPWritableSession>()
    private val peerMapping = mutableSetOf<PeerNyms>()

    companion object {
        private val logger = loggerFor<SessionManager>()
    }

    fun createWritableSession(channel: AsynchronousSocketChannel): TCPWritableSession {
        val session = TCPWritableSession(channel)
        logger.info("Created new writable session with ${session.getAddress()}")

        return session
    }

    fun removeWritableSession(writeNym: Address) = writableSessions.remove(writeNym)

    fun putWritableSession(writeNym: Address, session: TCPWritableSession) {
        writableSessions[writeNym] = session
    }

    fun getWritableSession(peer: Address): TCPWritableSession? {
        val writeNym = getPeerWriteNym(peer)

        return writableSessions[writeNym]
    }

    fun createReadableSession(channel: AsynchronousSocketChannel): TCPReadableSession {
        val session = TCPReadableSession(channel)
        logger.info("Created new readable session with ${session.getAddress()}")

        return session
    }

    fun removeReadableSession(readNym: Address) = readableSessions.remove(readNym)

    fun putReadableSession(readNym: Address, session: TCPReadableSession) {
        readableSessions[readNym] = session
    }

    fun getReadableSession(peer: Address): TCPReadableSession? {
        val readNym = getPeerReadNym(peer)

        return readableSessions[readNym]
    }

    fun addPeerMapping(readNym: Address, writeNym: Address) {
        peerMapping.add(PeerNyms(readNym, writeNym))
    }

    fun getPeerReadNym(peer: Address): Address? {
        val mapping = peerMapping.find { it.readNym == peer || it.writeNym == peer }
                ?: return null

        return if (mapping.readNym == peer) peer else mapping.readNym
    }

    fun getPeerWriteNym(peer: Address): Address? {
        val mapping = peerMapping.find { it.readNym == peer || it.writeNym == peer }
                ?: return null

        return if (mapping.writeNym == peer) peer else mapping.writeNym
    }
}