package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }
//
//    private val limiters: Map<PaymentExternalSystemAdapter, SlidingWindowRateLimiter> =
//        paymentAccounts.associateWith {
//            SlidingWindowRateLimiter(rate = 5, window = Duration.ofMillis(1000))
//        }


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
//            val limiter = limiters[account] ?: continue
//            limiter.tickBlocking()
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}