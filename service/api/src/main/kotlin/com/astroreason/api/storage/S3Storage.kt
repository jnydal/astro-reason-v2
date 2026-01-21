package com.astroreason.api.storage

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.*
import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.net.url.Url
import com.astroreason.core.Config
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import java.time.Instant

class S3Storage(
    private val bucket: String,
    private val endpoint: String?,
    private val accessKey: String?,
    private val secretKey: String?
) {
    private val s3Client = S3Client {
        endpoint?.let {
            endpointUrl = Url.parse(it)
        }
        forcePathStyle = true
        accessKey?.let {
            credentialsProvider = StaticCredentialsProvider(
                Credentials(it, secretKey ?: "")
            )
        }
        region = "us-east-1" // MinIO ignores but SDK requires
    }

    fun ensureBucket() {
        runBlocking {
            try {
                s3Client.headBucket(HeadBucketRequest {
                    bucket = this@S3Storage.bucket
                })
            } catch (e: Exception) {
                s3Client.createBucket(CreateBucketRequest {
                    bucket = this@S3Storage.bucket
                    acl = BucketCannedAcl.Private
                })
            }
        }
    }

    fun putBytes(namespace: String, content: ByteArray, contentType: String = "application/xml"): String {
        val hash = MessageDigest.getInstance("SHA-256")
            .digest(content)
            .take(16)
            .joinToString("") { "%02x".format(it) }
        
        val timestamp = Instant.now().toEpochMilli()
        val key = "$namespace/$hash-$timestamp.xml"
        
        runBlocking {
            s3Client.putObject(PutObjectRequest {
                bucket = this@S3Storage.bucket
                this.key = key
                body = ByteStream.fromBytes(content)
                this.contentType = contentType
            })
        }
        
        return "s3://$bucket/$key"
    }
}

fun createS3Storage(): S3Storage {
    val settings = Config.settings
    return S3Storage(
        bucket = settings.s3Bucket,
        endpoint = settings.s3Endpoint,
        accessKey = settings.s3AccessKey,
        secretKey = settings.s3SecretKey
    )
}
