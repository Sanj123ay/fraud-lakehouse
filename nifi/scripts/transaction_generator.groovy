// ============================================================================
//  STEP 2 FILE — NiFi ExecuteScript (Groovy): fake transaction generator
//  ---------------------------------------------------------------------
//  WHAT THIS FILE WILL CONTAIN (we write it together in Step 2):
//
//  A Groovy script that fabricates one realistic credit-card transaction
//  per run and returns it as a JSON string, e.g.:
//
//  {
//    "txn_id": "a1b2c3...",          // random UUID
//    "card_id": "CARD-42",            // small pool -> some cards get hot
//    "customer_id": "CUST-002",       // matches seed data in the bank DB
//    "merchant_id": "MER-300",        // matches seed merchants
//    "amount": 129.50,                // log-normal-ish; a few HUGE ones
//    "currency": "USD",
//    "country": "US",                 // occasionally != customer's home
//    "channel": "pos|ecom|atm",
//    "txn_ts": "2026-01-15T03:44:12Z",
//    "is_fraud": 0                    // HIDDEN LABEL used later for training;
//  }                                  // injected patterns: night + abroad +
//                                     // high amount + rapid repeats = 1
//
//  WHY WE DELIBERATELY PLANT FRAUD PATTERNS:
//  So the ML model in Step 5 has real signal to learn, and the streaming
//  scores in Step 6 light up on screen when an "attack" happens.
//
//  The NiFi flow around this script is documented in ../flow/README.md.
// ============================================================================


// SCRIPTS START HERE 

// =============================================================================
// NiFi ExecuteScript (Groovy) — realistic credit-card transaction generator
//
// Input : one trigger FlowFile from GenerateFlowFile
// Output: that FlowFile's content replaced by one JSON transaction
//
// The field names and types match spark_jobs/common/schemas.py exactly.
// Fraud is simulated on purpose so our Step 5 ML model has signal to learn.
// =============================================================================
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
def flowFile = session.get()
if (flowFile == null) {
    return
}
try {
    def random = ThreadLocalRandom.current()
    // These IDs match docker/postgres-source/init/01_schema.sql.
    // Customer home_country and merchant risk are repeated here ONLY to create
    // realistic events. Spark enriches from the authoritative Iceberg dims.
    def customers = [
        [id: 'CUST-001', homeCountry: 'IN'],
        [id: 'CUST-002', homeCountry: 'US'],
        [id: 'CUST-003', homeCountry: 'SG'],
        [id: 'CUST-004', homeCountry: 'US'],
        [id: 'CUST-005', homeCountry: 'AE']
    ]
    def regularMerchants = ['MER-100', 'MER-200', 'MER-500']
    def highRiskMerchants = ['MER-300', 'MER-400']
    def allCountries = ['US', 'IN', 'SG', 'AE', 'GB', 'DE']
    def currencies = [US: 'USD', IN: 'INR', SG: 'SGD', AE: 'AED', GB: 'GBP', DE: 'EUR']
    def customer = customers[random.nextInt(customers.size())]
    def cardNumber = random.nextInt(1, 5)
    def cardId = String.format('CARD-%s-%02d', customer.id.substring(5), cardNumber)
    // Overridable from docker-compose/.env. Default: six frauds per 100 rows.
    def configuredRate = System.getenv('SIMULATED_FRAUD_RATE') ?: '0.06'
    double fraudRate
    try {
        fraudRate = Double.parseDouble(configuredRate)
    } catch (NumberFormatException ignored) {
        fraudRate = 0.06d
    }
    fraudRate = Math.max(0.0d, Math.min(1.0d, fraudRate))
    boolean isFraud = random.nextDouble() < fraudRate
    String merchantId
    String country
    String channel
    double amount
    if (isFraud) {
        // Planted fraud signature: unusually large, e-commerce transaction at
        // a risky merchant, usually outside the customer's home country.
        merchantId = highRiskMerchants[random.nextInt(highRiskMerchants.size())]
        def foreignCountries = allCountries.findAll { it != customer.homeCountry }
        country = foreignCountries[random.nextInt(foreignCountries.size())]
        channel = random.nextDouble() < 0.85d ? 'ecom' : 'atm'
        amount = random.nextDouble(700.0d, 9000.0d)
    } else {
        // Normal spend: mostly local, mostly POS, usually under 500.
        merchantId = regularMerchants[random.nextInt(regularMerchants.size())]
        country = random.nextDouble() < 0.90d
            ? customer.homeCountry
            : allCountries[random.nextInt(allCountries.size())]
        double channelDraw = random.nextDouble()
        channel = channelDraw < 0.68d ? 'pos' : (channelDraw < 0.93d ? 'ecom' : 'atm')
        // A right-skewed amount distribution: many small purchases, few large.
        amount = Math.exp(3.35d + random.nextGaussian() * 0.78d)
        amount = Math.max(1.0d, Math.min(1200.0d, amount))
    }
    // Most events are current. Some are 1–8 minutes late so Step 3 can
    // demonstrate event-time watermarks. Very few arrive >10 minutes late.
    Instant eventTime = Instant.now()
    double latenessDraw = random.nextDouble()
    if (latenessDraw < 0.04d) {
        eventTime = eventTime.minus(random.nextLong(1L, 9L), ChronoUnit.MINUTES)
    } else if (latenessDraw < 0.045d) {
        eventTime = eventTime.minus(random.nextLong(11L, 21L), ChronoUnit.MINUTES)
    }
    String txnId = UUID.randomUUID().toString()
    def transaction = [
        txn_id     : txnId,
        card_id    : cardId,
        customer_id: customer.id,
        merchant_id: merchantId,
        amount     : Math.round(amount * 100.0d) / 100.0d,
        currency   : currencies[country],
        country    : country,
        channel    : channel,
        txn_ts     : eventTime.toString(),
        is_fraud   : isFraud ? 1 : 0
    ]
    String json = JsonOutput.toJson(transaction)
    flowFile = session.write(flowFile, { outputStream ->
        outputStream.write(json.getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    // PublishKafka_2_6 reads kafka.key automatically when Kafka Key is blank.
    // Same-card events then consistently hash to the same Kafka partition.
    flowFile = session.putAllAttributes(flowFile, [
        'mime.type'      : 'application/json',
        'filename'       : 'transaction-${txnId}.json',
        'kafka.key'      : cardId,
        'transaction.id' : txnId,
        'data.source'    : 'nifi-simulator',
        'schema.version' : '1',
        'fraud.simulated': isFraud ? 'true' : 'false'
    ])
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception error) {
    log.error('Could not generate simulated transaction', error)
    flowFile = session.putAttribute(flowFile, 'generation.error', error.toString())
    session.transfer(flowFile, REL_FAILURE)
}
