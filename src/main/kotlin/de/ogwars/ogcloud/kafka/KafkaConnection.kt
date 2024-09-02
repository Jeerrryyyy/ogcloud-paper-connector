package de.ogwars.ogcloud.kafka

import com.google.gson.Gson
import de.ogwars.ogcloud.message.KafkaMessage
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Future

class KafkaConnection(
    bootstrapServers: String,
    private val gson: Gson
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private var producer: KafkaProducer<String, String>

    init {
        val properties = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        producer = KafkaProducer<String, String>(properties)
    }

    fun <T : KafkaMessage> produceMessage(topic: String, message: T): Future<RecordMetadata> =
        producer.send(ProducerRecord(topic, gson.serializeKafkaValue(message))) { metadata, exception ->
            if (exception != null) {
                logger.info("Error sending message: ${exception.message}")
            } else {
                logger.info("Message sent to topic ${metadata.topic()} at offset ${metadata.offset()}")
            }
        }
}

fun <T : KafkaMessage> Gson.serializeKafkaValue(value: T): String = toJson(value)
