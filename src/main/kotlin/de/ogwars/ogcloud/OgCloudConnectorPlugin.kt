package de.ogwars.ogcloud

import com.google.gson.GsonBuilder
import de.ogwars.ogcloud.kafka.KafkaConnection
import de.ogwars.ogcloud.message.ServerReadyMessage
import de.ogwars.ogcloud.message.ServerRemoveMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.bukkit.plugin.java.JavaPlugin
import org.kodein.di.DI
import org.kodein.di.bindSingleton
import org.kodein.di.direct
import org.kodein.di.instance
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

@Suppress("unused") // Used by the plugin system of paper.
class OgCloudConnectorPlugin : JavaPlugin(), CoroutineScope {
    override val coroutineContext: CoroutineContext = Dispatchers.Default

    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        lateinit var KODEIN: DI
    }

    override fun onEnable() {
        initializeDI()

        // TODO: Everything else before!!!
        this.server.scheduler.runTaskLaterAsynchronously(this, Runnable {
            val serverName = System.getenv("OGCLOUD_SERVER_NAME")
            val clusterAddress = System.getenv("OGCLOUD_CLUSTER_ADDRESS")

            logger.info("Trying to connect to velocity proxy... (ServerName: $serverName, ClusterAddress: $clusterAddress)")

            KODEIN.direct.instance<KafkaConnection>().produceMessage(
                "servers-ready",
                ServerReadyMessage(serverName, clusterAddress)
            )
        }, 20L * 5)
    }

    override fun onDisable() {

        // TODO: Everything else before!!!
        val serverName = System.getenv("OGCLOUD_SERVER_NAME")
        KODEIN.direct.instance<KafkaConnection>().produceMessage(
            "servers-remove",
            ServerRemoveMessage(serverName)
        )
    }

    private fun initializeDI() {
        KODEIN = DI {
            bindSingleton { this@OgCloudConnectorPlugin }
            bindSingleton { GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create() }
            bindSingleton {
                KafkaConnection(
                    // TODO: Fetch through config
                    "ogcloud-kafka-0.ogcloud-kafka.default.svc.cluster.local:9092",
                    instance()
                )
            }
        }
    }
}