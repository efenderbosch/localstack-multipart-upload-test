import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest
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
import java.time.Instant

internal class LocalStackS3MultiPartPresignTest {

    companion object {
        private const val bucketName = "test-bucket"
        private val key = "random-${Instant.now()}.bin"
        private val logger = LoggerFactory.getLogger(LocalStackS3MultiPartPresignTest::class.java)
    }

    @Test
    fun `test presign multipart using localstack `() {
        val localstackEndpoint = URI("http://127.0.0.1:4566")
        val credentials = AwsBasicCredentials.create("test", "test")
        val credentialsProvider = StaticCredentialsProvider.create(credentials)

        val s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .endpointOverride(localstackEndpoint)
            .build()

        // create bucket in localstack if it doesn't exist
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
            val eTags = uploadPartsUsingPresignedUrls(presignedUrls)
            completeMultiPartUpload(uploadId, eTags, s3Client)
            val headObjectResponse = HeadObjectRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .build()
                .let { s3Client.headObject(it) }

            val tags = GetObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
                .let { s3Client.getObjectTagging(it) }
                .tagSet()

            val tagPresent = tags.any { tag -> tag.key() == "key" && tag.value() == "value" }

            assertAll(
                { assertTrue(headObjectResponse.contentLength() == 12582912L) { "content-length" } },
                { assertTrue(headObjectResponse.contentType() == "application/octet-stream") { "content-type" } },
                // this assertion fails using localstack:0.12.7, but succeeds for real S3
                { assertTrue(tagPresent, "tags should include 'key=value'; found: $tags") }
            )
        } finally {
            cleanUp(s3Client)
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
        logger.info("using AWS access key {}", credentialsProvider.resolveCredentials().accessKeyId())

        val s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .build()

        val s3Presigner = S3Presigner.builder()
            .credentialsProvider(credentialsProvider)
            .build()

        val bucketExists = s3Client.listBuckets().buckets().any { it.name() == bucketName }
        assert(bucketExists) { "bucket $bucketName does not exist in S3, please create it" }

        val (uploadId, presignedUrls) = presign(s3Client, s3Presigner)
        try {
            val eTags = uploadPartsUsingPresignedUrls(presignedUrls)
            completeMultiPartUpload(uploadId, eTags, s3Client)
            val headObjectResponse = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
                .let { s3Client.headObject(it) }

            val tags = GetObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
                .let { s3Client.getObjectTagging(it) }
                .tagSet()

            val tagPresent = tags.any { tag -> tag.key() == "key" && tag.value() == "value" }

            assertAll(
                { assertTrue(headObjectResponse.contentLength() == 12582912L) { "content-length" } },
                { assertTrue(headObjectResponse.contentType() == "application/octet-stream") { "content-type" } },
                { assertTrue(tagPresent, "tags should include 'key=value'; found: $tags") }
            )
        } finally {
            cleanUp(s3Client)
        }
    }

    private fun presign(s3Client: S3Client, s3Presigner: S3Presigner): Pair<String, List<URL>> {
        val tag = Tag.builder().key("key").value("value").build()
        val tags = Tagging.builder().tagSet(tag).build()

        val uploadId = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .contentType("application/octet-stream")
            .key(key)
            .tagging(tags)
            .build()
            .let { s3Client.createMultipartUpload(it) }
            .uploadId()

        val urls = (1..2).map { partNumber ->
            val uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key)
                .partNumber(partNumber)
                .uploadId(uploadId)
                .build()

            val presignedUploadPartRequest = UploadPartPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(15))
                .uploadPartRequest(uploadPartRequest)
                .build()
                .let { s3Presigner.presignUploadPart(it) }

            logger.info("presigned url: {}", presignedUploadPartRequest.url())
            logger.info("signed headers: {}", presignedUploadPartRequest.signedHeaders())
            presignedUploadPartRequest.url()
        }
        return uploadId to urls
    }

    private fun uploadPartsUsingPresignedUrls(presignedUrls: List<URL>): Map<Int, String> {
        // random.bin was generated w/ "dd if=/dev/urandom of=random.bin bs=4096 count=3072"
        val file = RandomAccessFile("src/test/resources/random.bin", "r")
        val size = file.length().toInt()

        val partOneSize = size / 2
        val partTwoSize = size - partOneSize

        val partOneByteArray = ByteArray(partOneSize)
        file.read(partOneByteArray)

        val partTwoByteArray = ByteArray(partTwoSize)
        file.read(partTwoByteArray)

        val partOneEtag = sendPart(presignedUrls[0], partOneByteArray)
        val partTwoEtag = sendPart(presignedUrls[1], partTwoByteArray)
        return mapOf(1 to partOneEtag, 2 to partTwoEtag)
    }

    private fun sendPart(url: URL, bytes: ByteArray): String {
        val connection = url.openConnection() as HttpURLConnection
        connection.requestMethod = "PUT"
        connection.doOutput = true
        connection.addRequestProperty("content-type", "application/octet-stream")
        connection.outputStream.use { outputStream ->
            outputStream.write(bytes)
            outputStream.flush()
        }
        // upload fails signature validation using localstack:latest (as of 2021-03-12)
        // upload succeeds using localstack:0.12.7
        logger.info("response: {}", connection.responseCode)
        connection.headerFields
            .filter { (name, value) -> name != null && value != null }
            .forEach { (name, value) -> logger.info("header {} = {}", name, value) }
        return connection.getHeaderField("Etag")
    }

    private fun completeMultiPartUpload(uploadId: String, eTags: Map<Int, String>, s3Client: S3Client) {
        val completedMultipartUpload = eTags.entries
            .map { (part, eTag) ->
                CompletedPart.builder().partNumber(part).eTag(eTag).build()
            }.let { parts ->
                CompletedMultipartUpload.builder().parts(parts).build()
            }

        CompleteMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build()
            .let { s3Client.completeMultipartUpload(it) }
    }

    private fun cleanUp(s3Client: S3Client) {
        logger.info("cleaning up")
        val multiPartUploads = ListMultipartUploadsRequest.builder()
            .bucket(bucketName)
            .prefix("random-")
            .build()
            .let { s3Client.listMultipartUploads(it) }
            .uploads()

        multiPartUploads.forEach { multiPartUpload ->
            logger.info("aborting multipart upload {}", multiPartUpload.key())
            AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .uploadId(multiPartUpload.uploadId())
                .key(multiPartUpload.key())
                .build()
                .let { s3Client.abortMultipartUpload(it) }
        }

        DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .build()
            .let { s3Client.deleteObject(it) }
    }
}
