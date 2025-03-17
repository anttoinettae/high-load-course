package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestQueue = ConcurrentLinkedQueue<PaymentRequest>()
    private val executor = Executors.newFixedThreadPool(parallelRequests)

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    private val client = OkHttpClient.Builder()
        .connectTimeout(6, TimeUnit.SECONDS)
        .writeTimeout(6, TimeUnit.SECONDS)
        .readTimeout(6, TimeUnit.SECONDS)
        .build()
    private val hedgedExecutor = Executors.newCachedThreadPool()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Adding payment request for payment $paymentId to queue")
        requestQueue.add(PaymentRequest(paymentId, amount, paymentStartedAt, deadline))
        processQueue()
    }

    private fun processQueue() {
        while (requestQueue.isNotEmpty()) {
            val request = requestQueue.poll() ?: return
            executor.submit { executePayment(request) }
        }
    }

    private fun executePayment(request: PaymentRequest) {
        limiter.tickBlocking()
        val transactionId = UUID.randomUUID()
        val idempotencyKey = UUID.randomUUID().toString()

        logger.info("[$accountName] Processing payment for ${request.paymentId}, txId: $transactionId")

        paymentESService.update(request.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - request.paymentStartedAt))
        }

        val httpRequest = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${request.paymentId}&amount=${request.amount}")
            .post(emptyBody)
            .header("x-idempotency-key", idempotencyKey)
            .build()

        val call1 = client.newCall(httpRequest)
        val future1 = hedgedExecutor.submit<okhttp3.Response> { call1.execute() }

        Thread.sleep(2000)

        if (!future1.isDone) {
            logger.warn("[$accountName] Первый запрос медленный, запускаем hedged request, txId: $transactionId")
            val call2 = client.newCall(httpRequest)
            val future2 = hedgedExecutor.submit<okhttp3.Response> { call2.execute() }

            try {
                val response1 = future1.get(500, TimeUnit.MILLISECONDS)
                call2.cancel() // Если первый успел - отменяем второй
                handleResponse(response1, transactionId, request)
            } catch (e: Exception) {
                val response2 = future2.get() // Если первый не успел - ждём второй
                call1.cancel()
                handleResponse(response2, transactionId, request)
            }
        } else {
            val response1 = future1.get()
            handleResponse(response1, transactionId, request)
        }
    }

    private fun handleResponse(response: okhttp3.Response, transactionId: UUID, request: PaymentRequest) {
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${request.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(transactionId.toString(), request.paymentId.toString(), false, e.message)
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}")

        paymentESService.update(request.paymentId) {
            it.logProcessing(body.result, now(), transactionId, reason = body.message)
        }

        processQueue()
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

public fun now() = System.currentTimeMillis()