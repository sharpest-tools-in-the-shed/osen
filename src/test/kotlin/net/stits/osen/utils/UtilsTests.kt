package net.stits.osen.utils

import net.stits.osen.CompressionUtils
import net.stits.osen.SerializationUtils
import org.junit.Test
import java.nio.charset.StandardCharsets
import java.util.*

class UtilsTests {
    @Test
    fun `serialization and deserialization works`() {
        val data = SampleDataClass()

        val serializedDataToBytes = SerializationUtils.anyToBytes(data)
        val deserializedDataFromBytes = SerializationUtils.bytesToAny<SampleDataClass>(serializedDataToBytes)

        assert(data == deserializedDataFromBytes)

        val serializedDataToJSON = SerializationUtils.anyToJSON(data)
        val deserializedDataFromJSON = SerializationUtils.jSONToAny<SampleDataClass>(serializedDataToJSON)

        assert(data == deserializedDataFromJSON)
    }

    @Test
    fun `compression and decompression works`() {
        val data = SampleDataClass()

        val serializedData = SerializationUtils.anyToBytes(data)
        val compressedAndSerializedData = CompressionUtils.compress(serializedData)
        val decompressedAndSerializedData = CompressionUtils.decompress(compressedAndSerializedData)
        val decompressedAndDeserializedData = SerializationUtils.bytesToAny<SampleDataClass>(decompressedAndSerializedData)

        assert(Arrays.equals(decompressedAndSerializedData, serializedData))
        assert(decompressedAndDeserializedData == data)
    }
}


data class SampleDataClass(
        val muchText: String = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc tempor tortor ipsum, a tempor ante efficitur ac. Curabitur blandit libero a ex pretium, vitae faucibus ipsum suscipit. Ut posuere fringilla consequat. Suspendisse a risus vulputate, ultricies ante et, porttitor mi. Nam feugiat mi eget orci volutpat finibus. Curabitur sodales accumsan aliquam. Duis sollicitudin aliquet urna, vel commodo diam pulvinar id.\n" +
                "\n" +
                "Ut facilisis tempus eleifend. Proin et elementum est, sit amet semper risus. Mauris sed nisi sit amet justo dapibus fringilla. Praesent sed auctor odio. Vivamus vitae odio vitae lacus posuere tempus. Proin sit amet porta lectus. Pellentesque gravida ultricies felis, eget rhoncus erat pellentesque ut. Ut magna orci, pellentesque at ante vel, volutpat ornare nunc. Vestibulum sed ante pellentesque, pellentesque orci eget, sollicitudin eros. Proin ut arcu odio. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos.\n" +
                "\n" +
                "Morbi dictum mattis erat, ut tristique nisl. Nulla id dolor ultrices, sollicitudin ante vel, vestibulum nisi. Curabitur quis quam non nibh luctus interdum. In quis turpis ornare, posuere odio in, egestas odio. Maecenas vestibulum est quis tellus tempor, sed consequat nulla vestibulum. In eu fringilla dui. Fusce imperdiet laoreet vulputate. Sed at ligula nulla. Duis quis bibendum magna. Praesent posuere aliquet enim eu bibendum. Mauris ac volutpat lacus. Morbi pretium quam neque, ultrices viverra eros ultrices sed. Donec viverra luctus imperdiet. Duis mattis turpis in justo pellentesque, eget consequat nibh luctus.\n" +
                "\n" +
                "Maecenas placerat nisl eget neque rhoncus cursus vitae mollis augue. Donec sed egestas ex. Proin a suscipit magna. Nulla et porta dui. Duis nec nulla ac justo rutrum ultrices in nec velit. Ut maximus maximus massa nec facilisis. Integer eu volutpat odio. Quisque malesuada a turpis ac vehicula. Quisque elit ante, posuere eget libero non, consequat ultrices ante. Aliquam in lectus mauris.\n" +
                "\n" +
                "Nunc lorem magna, dignissim non magna eget, accumsan pretium ipsum. Integer at tortor vitae sapien porttitor sagittis id nec diam. Aenean nec orci non metus placerat fringilla. Nunc venenatis dictum porttitor. Vivamus ut metus a libero aliquet sollicitudin. Donec est ligula, iaculis quis nisi a, scelerisque gravida turpis. Sed imperdiet purus ut consectetur dictum. Praesent luctus efficitur finibus. Pellentesque et commodo augue. Praesent sed orci vitae nisl pellentesque rhoncus.",
        val long: Long = 123123L,
        val int: Int = 123,
        val randomBytes: ByteArray = muchText.toByteArray(StandardCharsets.UTF_8)
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SampleDataClass

        if (muchText != other.muchText) return false
        if (long != other.long) return false
        if (int != other.int) return false
        if (!Arrays.equals(randomBytes, other.randomBytes)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = muchText.hashCode()
        result = 31 * result + long.hashCode()
        result = 31 * result + int
        result = 31 * result + Arrays.hashCode(randomBytes)
        return result
    }
}
