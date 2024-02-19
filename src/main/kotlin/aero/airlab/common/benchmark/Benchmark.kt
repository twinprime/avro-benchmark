package aero.airlab.common.benchmark

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.io.AvroDecodeFormat
import com.github.avrokotlin.avro4k.io.AvroEncodeFormat
import com.github.avrokotlin.avro4k.serializer.InstantSerializer
import kotlinx.serialization.Serializable
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.avro.reflect.Union
import java.io.ByteArrayOutputStream
import java.time.Instant

@Serializable
data class TestRecord(
    val id: Long,
    val description: String,
    @Serializable(with = InstantSerializer::class)
    val creationDt: Instant,
    val records: List<SubRecord>,
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@Union(StringSubRecord::class, InstantSubRecord::class)
@Serializable
sealed interface SubRecord {
    val id: Long
}

@Serializable
data class StringSubRecord(override val id: Long, val value: String) : SubRecord
@Serializable
data class InstantSubRecord(override val id: Long,
                            @Serializable(with = InstantSerializer::class)
                            val value: Instant) : SubRecord

object Benchmark {
    @JvmStatic
    fun main(args: Array<String>) {
        val reps = 2
        val record = TestRecord(1000_000_000,
            "testing 1 2 3 4", Instant.now(), listOf(
                StringSubRecord(1, "test1"),
                InstantSubRecord(2, Instant.now()),
            ))

        val (avroBytes, avroAvg) = avroSerialization(record, reps)
        val (jacksonStr, jsonAvg) = jacksonSerialization(record, reps)
        val (avroJacksonBytes, avroJacksonAvg) = avroJacksonSerialization(record, reps)
        val (avro4kBytes, avro4kAvg) = avro4kSerialization(record, reps)
        println("Diff: ${jsonAvg - avroAvg}, ${avroJacksonAvg - avroAvg}, ${avro4kAvg - avroAvg}")

        val avroDesAvg = avroDeserialization(reps, avroBytes)
        val jsonDesAvg = jacksonDeserialization(reps, jacksonStr)
        val avroJacksonDesAvg = avroJacksonDeserialization(reps, avroJacksonBytes)
        val avro4kDesAvg = avro4kDeserialization(reps, avro4kBytes)
        println("Diff: ${jsonDesAvg - avroDesAvg}, ${avroJacksonDesAvg - avroDesAvg}, ${avro4kDesAvg - avroDesAvg}")
    }

    private fun avroDeserialization(reps: Int, avroBytes: ByteArray): Long {
        val schema = ReflectData.get().getSchema(TestRecord::class.java)
        val reader = ReflectDatumReader<TestRecord>(schema)
        var rec: TestRecord? = null
        val avg = runBenchmark("Avro deserialization", reps) {
            val decoder = DecoderFactory.get().binaryDecoder(avroBytes, null)
            rec = reader.read(null, decoder)
        }
        println("Avro deserialized: $rec")
        return avg
    }

    private fun avroJacksonDeserialization(reps: Int, avroBytes: ByteArray): Long {
        val schemaMapper = ObjectMapper(AvroFactory())
            .registerKotlinModule()
            .registerModule(AvroJavaTimeModule())
        val gen = AvroSchemaGenerator()
        schemaMapper.acceptJsonFormatVisitor(TestRecord::class.java, gen)

        val mapper = AvroMapper().registerModule(kotlinModule()).registerModule(AvroJavaTimeModule())
        val reader = mapper.readerFor(TestRecord::class.java).with(gen.generatedSchema)
        var rec: TestRecord? = null
        val avroJacksonAvg = runBenchmark("AvroJackson deserialization", reps) {
            rec = reader.readValue(avroBytes)
        }
        println("AvroJackson deserialized: $rec")
        return avroJacksonAvg
    }

    private fun avro4kDeserialization(reps: Int, avroBytes: ByteArray): Long {
        val schema = Avro.default.schema(TestRecord.serializer())
        val isBuilder = Avro.default.openInputStream(TestRecord.serializer()) {
            decodeFormat = AvroDecodeFormat.Binary(schema)
        }
        var rec: TestRecord? = null
        val avro4kDesAvg = runBenchmark("Avro4k deserialization", reps) {
            rec = isBuilder.from(avroBytes).use { it.nextOrThrow() }
        }
        println("Avro4k deserialized: $rec")
        return avro4kDesAvg
    }

    private fun jacksonDeserialization(reps: Int, jacksonStr: String): Long {
        val objMapper= jacksonObjectMapper().registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        val jsonAvg = runBenchmark("Jackson deserialization", reps) {
            objMapper.readValue<TestRecord>(jacksonStr)
        }
        return jsonAvg
    }

    private fun avroSerialization(record: TestRecord, reps: Int): Pair<ByteArray, Long> {
        val schema = ReflectData.get().getSchema(TestRecord::class.java)
        val writer = ReflectDatumWriter<TestRecord>(schema)
        var bytes = ByteArray(0)
        val avg = runBenchmark("Avro serialization", reps) {
            val out = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(out, null)
            writer.write(record, encoder)
            encoder.flush()
            bytes = out.toByteArray()
        }
        println("Avro size: ${bytes.size}")
        return Pair(bytes, avg)
    }

    private fun avroJacksonSerialization(record: TestRecord, reps: Int): Pair<ByteArray, Long> {
        val schemaMapper = ObjectMapper(AvroFactory())
            .registerKotlinModule()
            .registerModule(AvroJavaTimeModule())
        val gen = AvroSchemaGenerator()
        schemaMapper.acceptJsonFormatVisitor(TestRecord::class.java, gen)

        val mapper = AvroMapper().registerModule(kotlinModule()).registerModule(AvroJavaTimeModule())
        val writer = mapper.writer(gen.generatedSchema)
        var avroBytesJackson = ByteArray(0)
        val avro4kAvg = runBenchmark("AvroJackson serialization", reps) {
            avroBytesJackson = writer.writeValueAsBytes(record)
        }
        println("AvroJackson size: ${avroBytesJackson.size}")
        return Pair(avroBytesJackson, avro4kAvg)
    }

    private fun avro4kSerialization(record: TestRecord, reps: Int): Pair<ByteArray, Long> {
        val osBuilder = Avro.default.openOutputStream(TestRecord.serializer()) {
            encodeFormat = AvroEncodeFormat.Binary
            schema = Avro.default.schema(TestRecord.serializer())
        }
        var avroBytes4k = ByteArray(0)
        val avro4kAvg = runBenchmark("Avro4k serialization", reps) {
            val os = ByteArrayOutputStream()
            osBuilder.to(os).write(record).close()
            avroBytes4k = os.toByteArray()
        }
        println("Avro4k size: ${avroBytes4k.size}")
        return Pair(avroBytes4k, avro4kAvg)
    }

    private fun jacksonSerialization(record: TestRecord, reps: Int): Pair<String, Long> {
        val objMapper= jacksonObjectMapper().registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        val jacksonStr = objMapper.writeValueAsString(record)
        println("Jackson string: $jacksonStr")
        val jsonAvg = runBenchmark("Jackson serialization", reps) {
            objMapper.writeValueAsString(record)
        }
        return Pair(jacksonStr, jsonAvg)
    }

    private fun runBenchmark(id: String, reps: Int, workload: () -> Unit): Long {
        repeat(reps / 2) { workload() }
        val startTime = System.nanoTime()
        repeat(reps) { workload() }
        val timeTaken = System.nanoTime() - startTime
        val avg = timeTaken / reps
        println("$id: $avg ns")
        return avg
    }

}