import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.Tag
import software.amazon.awssdk.services.s3.model.Tagging
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest
import java.io.RandomAccessFile
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.time.Duration

internal class LocalStackS3PresignTest {

    companion object {
        private const val bucketName = "hubindustrial-iq-files-test"
        private val logger by lazy { LoggerFactory.getLogger(LocalStackS3PresignTest::class.java) }
    }

    @Test
    fun `test presign multipart using localstack`() {
        val localstackEndpoint = URI("http://localstack:4566")
        val credentials = AwsBasicCredentials.create("test", "test")
        val credentialsProvider = StaticCredentialsProvider.create(credentials)

        val s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .endpointOverride(localstackEndpoint)
            .build()

        val bucketExists = s3Client.listBuckets().buckets().any { it.name() == bucketName }
        if (!bucketExists) {
            CreateBucketRequest.builder().bucket(bucketName).build().let { s3Client.createBucket(it) }
        }

        val s3Presigner = S3Presigner.builder()
            .credentialsProvider(credentialsProvider)
            .endpointOverride(localstackEndpoint)
            .build()

        val (uploadId, presignedUrls) = presign(s3Client, s3Presigner)
        try {
            val eTags = testPresignedUrls(presignedUrls)
            completeMultiPartUpload(uploadId, eTags, s3Client)
            val headObjectResponse = HeadObjectRequest
                .builder()
                .bucket(bucketName)
                .key("random.bin")
                .build()
                .let { s3Client.headObject(it) }

            assert(headObjectResponse.contentLength() == 12582912L)
            assert(headObjectResponse.contentType() == "application/octet-stream")
        } finally {
            deleteFile(s3Client)
        }
    }

    @Test
    fun `test presign multi part using S3`() {
        // DefaultCredentialsProvider will load credentials from:
        // sys props aws.accessKeyId & aws.secretAccessKey or
        // env vars AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY or
        // profile
        // for profile:
        // set sys prop aws.profile or env var AWS_PROFILE to use non-default profile from ~/.aws/credentials or
        // set sys prop aws.sharedCredentialsFile or env var AWS_SHARED_CREDENTIALS_FILE to use a different file
        val credentialsProvider = DefaultCredentialsProvider.create()
        val s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .build()
        val s3Presigner = S3Presigner.builder()
            .credentialsProvider(credentialsProvider)
            .build()

        val (uploadId, presignedUrls) = presign(s3Client, s3Presigner)
        try {
            val eTags = testPresignedUrls(presignedUrls)
            completeMultiPartUpload(uploadId, eTags, s3Client)
            val headObjectResponse = HeadObjectRequest
                .builder()
                .bucket(bucketName)
                .key("random.bin")
                .build()
                .let { s3Client.headObject(it) }
            assert(headObjectResponse.contentLength() == 12582912L)
            assert(headObjectResponse.contentType() == "application/octet-stream")
            assert(headObjectResponse.partsCount() == 2)
        } finally {
            deleteFile(s3Client)
        }
    }

    private fun presign(s3Client: S3Client, s3Presigner: S3Presigner): Pair<String, List<URL>> {
        val tag = Tag.builder().key("key").value("value").build()
        val tags = Tagging.builder().tagSet(tag).build()

        val uploadId = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .contentType("application/octet-stream")
            .key("random.bin")
            .tagging(tags)
            .build()
            .let { s3Client.createMultipartUpload(it) }
            .uploadId()

        val urls = (1..2).map { partNumber ->
            val uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key("random.bin")
                .partNumber(partNumber)
                .uploadId(uploadId)
                .build()

            val presignRequest = UploadPartPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(15))
                .uploadPartRequest(uploadPartRequest)
                .build()

            s3Presigner.presignUploadPart(presignRequest).url()
        }
        return uploadId to urls
    }

    private fun testPresignedUrls(presignedUrls: List<URL>): Map<Int, String> {
        // random.bin was generated w/ "dd if=/dev/urandom of=random.bin bs=4096 count=3072"
        val file = RandomAccessFile("src/test/resources/random.bin", "r")
        val size = file.length().toInt()

        val partOneSize = size / 2
        val partTwoSize = size - partOneSize

        val partOneByteArray = ByteArray(partOneSize)
        file.read(partOneByteArray)

        val partTwoByteArray = ByteArray(partTwoSize)
        file.read(partTwoByteArray)

        val partOneUrl = presignedUrls[0]
        val partTwoUrl = presignedUrls[1]

        val partOneEtag = sendPart(partOneUrl, partOneByteArray)
        val partTwoEtag = sendPart(partTwoUrl, partTwoByteArray)
        return mapOf(1 to partOneEtag, 2 to partTwoEtag)
    }

    private fun sendPart(url: URL, bytes: ByteArray): String {
        val connection = url.openConnection() as HttpURLConnection
        connection.requestMethod = "PUT"
        connection.doOutput = true
        connection.addRequestProperty("content-type", "application/octet-stream")
        // both localstack:latest and localstack:0.12.7 fail w/ this header, but S3 does not
        // this header is required to be present to have the correct tags on the object after all parts have been uploaded
        connection.addRequestProperty("x-amz-tagging", "key=value")
        connection.outputStream.use { outputStream ->
            outputStream.write(bytes)
            outputStream.flush()
        }
        logger.info("response: {}", connection.responseCode)
        connection.headerFields.forEach { (name, value) -> logger.info("header {} = {}", name, value) }
        return connection.getHeaderField("Etag")
    }

    private fun completeMultiPartUpload(uploadId: String, eTags: Map<Int, String>, s3Client: S3Client) {
        val completedMultipartUpload = eTags.entries
            .map { (part, eTag) ->
                CompletedPart.builder().partNumber(part).eTag(eTag).build()
            }.let { parts ->
                CompletedMultipartUpload.builder().parts(parts).build()
            }
        s3Client.completeMultipartUpload { completeMultiPartUploadRequestBuilder ->
            completeMultiPartUploadRequestBuilder
                .bucket(bucketName)
                .key("random.bin")
                .uploadId(uploadId)
                .multipartUpload(completedMultipartUpload)
                .build()
        }
    }

    private fun deleteFile(s3Client: S3Client) {
        s3Client.deleteObject { deleteObjectRequestBuilder ->
            deleteObjectRequestBuilder.bucket(bucketName).key("key").build()
        }
    }
}
