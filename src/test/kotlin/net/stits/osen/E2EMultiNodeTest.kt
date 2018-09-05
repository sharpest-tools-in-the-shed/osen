package net.stits.osen

import kotlinx.coroutines.experimental.runBlocking
import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import net.stits.osen.controller.TestPayload
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import java.util.*


data class Ports(val web: Int, val p2p: Int) {
    fun toProperties(): Properties {
        val props = Properties()
        props.setProperty("server.port", web.toString())
        props.setProperty("node.port", p2p.toString())

        return props
    }
}

fun createNodes(count: Int, baseWebPort: Int = 8080, baseP2PPort: Int = 1337): List<Ports> {
    val nodesPorts = arrayListOf<Ports>()
    for (i in 1..count) {
        nodesPorts.add(Ports(baseWebPort + i, baseP2PPort + i))
    }

    return nodesPorts
}

@RunWith(SpringJUnit4ClassRunner::class)
class E2EMultiNodeTest {
    lateinit var nodesPorts: List<Ports>
    private val host = "localhost"

    private val nodesCount = 10 // change this to change number of nodes

    private val apps = arrayListOf<SpringApplicationBuilder>()
    var appContexts: List<ConfigurableApplicationContext>? = null

    @Before
    fun initNodes() {
        nodesPorts = createNodes(nodesCount)

        nodesPorts.forEach { ports ->
            apps.add(SpringApplicationBuilder(TestApplication::class.java).properties(ports.toProperties()))
        }

        appContexts = apps.map { it.run() }
    }

    @After
    fun destroyNodes() {
        appContexts!!.forEach { it.close() }
    }

    @Test
    fun `all nodes are able to send requests to each other and get response`() {
        val p2ps = appContexts!!.map { it.getBean(P2P::class.java) }

        repeat(200) {
            p2ps.forEachIndexed { idx, p2p ->
                nodesPorts.forEach { ports ->
                    val receiver = Address(host, ports.p2p)

                    runBlocking {
                        val message = Message(TOPIC_TEST, TestMessageTypes.TEST, TestPayload())
                        val response = p2p.sendAndReceive<String>(receiver, message)

                        assert(response == "Test string payload") { "send() returned invalid response" }
                    }
                }
            }
        }
    }
}