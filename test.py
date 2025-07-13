import io
import json
import decimal
import time
from locust import User, task, between, events
from confluent_kafka import Producer, Consumer, KafkaError
import fastavro
import requests

# Configuration
BROKERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
INPUT_TOPIC = "effyis.partners.p2s.transactions.input"
OUTPUT_TOPIC = "effyis.partners.p2s.transactions.processed"

# Helper functions
def register_schema(subject, schema_str):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    resp = requests.post(url, json={"schema": schema_str})
    if resp.status_code == 409:
        latest = get_latest_schema(subject)
        return latest["id"]
    resp.raise_for_status()
    return resp.json()["id"]

def get_latest_schema(subject):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def decimal_to_bytes(dec, precision=20, scale=10):
    unscaled = int(dec * (10 ** scale))
    length = (unscaled.bit_length() + 8) // 8 or 1
    return unscaled.to_bytes(length, byteorder="big", signed=True)

def parse_schema(schema_str):
    return fastavro.parse_schema(json.loads(schema_str))

producer_conf = {"bootstrap.servers": BROKERS}
consumer_conf = {
    "bootstrap.servers": BROKERS,
    "group.id": "locust-consumer-group",
    "auto.offset.reset": "earliest",
}

class KafkaUser(User):
    wait_time = between(1, 3)

    def on_start(self):
        self.key_schema_str = """{
            "name": "KeySchema",
            "type": "record",
            "namespace": "effyis.partners.p2s.shared.avro.meta.input",
            "fields": [
                { "name": "id", "type": "string" }
            ]
        }"""

        self.value_schema_str = """{
            "type": "record",
            "name": "Input",
            "namespace": "effyis.partners.p2s.shared.avro.meta.input",
            "fields": [
                {
                    "name": "acceptor",
                    "type": {
                        "type": "record",
                        "name": "Acceptor",
                        "doc": "Information about the merchant accepting the transaction",
                        "fields": [
                            {"name": "id", "type": "string", "doc": "Unique ID of the merchant accepting the transaction (P2S-00001)"},
                            {"name": "subId", "type": ["null", "string"], "default": null, "doc": "Sub-merchant ID, used when there are multiple merchants under a single account (P2S-00002)"},
                            {"name": "terminalId", "type": "string", "doc": "ID of the terminal where the transaction was made (P2S-00003)"},
                            {"name": "pricingRuleCode", "type": ["null", "string"], "default": null, "doc": "Merchant pricing model applied by the acquirer or PSP (P2S-00004)"},
                            {"name": "merchantCategoryCode", "type": "string", "doc": "Merchant category code indicating the type of merchant (e.g., retail, restaurant) (P2S-00005)"},
                            {"name": "name", "type": ["null", "string"], "default": null, "doc": "The name of the merchant (P2S-00006)"},
                            {"name": "city", "type": ["null", "string"], "default": null, "doc": "The city where the merchant is located (P2S-00007)"},
                            {"name": "country", "type": "string", "doc": "The country where the merchant is located (P2S-00008)"},
                            {"name": "identifier", "type": ["null", "string"], "default": null, "doc": "Additional identifier for the merchant (e.g., NFC Field 55, TLV data) (P2S-00009)"},
                            {"name": "referenceNumber", "type": ["null", "string"], "default": null, "doc": "Internal reference number used by the merchant for the transaction (P2S-00010)"}
                        ]
                    },
                    "doc": "Merchant acceptor details"
                },
                {
                    "name": "acquirer",
                    "type": {
                        "type": "record",
                        "name": "Acquirer",
                        "doc": "Information about the acquiring institution",
                        "fields": [
                            {"name": "acquiringInstitutionId", "type": "string", "doc": "Bank ID of acquirer (P2S-00012)"},
                            {"name": "acquirerBin", "type": "string", "doc": "Bank Identification Number (P2S-00013)"},
                            {"name": "country", "type": "string", "doc": "Acquirer Country (P2S-00014)"}
                        ]
                    },
                    "doc": "Acquiring institution details"
                },
                {
                    "name": "card",
                    "type": {
                        "type": "record",
                        "name": "Card",
                        "doc": "Information about the card used in the transaction",
                        "fields": [
                            {"name": "issuerBin", "type": "string", "doc": "Issuer Bank Identification Number for detecting card type, region, and rules (P2S-00015)"}
                        ]
                    },
                    "doc": "Card details"
                },
                {
                    "name": "chargeback",
                    "type": ["null", {
                        "type": "record",
                        "name": "Chargeback",
                        "doc": "Information about any chargeback related to the transaction",
                        "fields": [
                            {"name": "id", "type": "string", "doc": "Chargeback transaction id (P2S-00017)"},
                            {"name": "originalId", "type": "string", "doc": "Original transaction Id (P2S-00018)"},
                            {"name": "cardholderdisputedate", "type": ["null", "string"], "default": null, "doc": "The date the cardholder initiated the dispute (P2S-00019)"},
                            {"name": "issuingDate", "type": ["null", "string"], "default": null, "doc": "The date the issuing bank received the chargeback (P2S-00020)"},
                            {"name": "amount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "The amount disputed by the cardholder (P2S-00021)"},
                            {"name": "reasonCode", "type": ["null", "string"], "default": null, "doc": "The reason code for the chargeback (P2S-00022)"},
                            {"name": "representment", "type": ["null", "string"], "default": null, "doc": "Information related to the chargeback representation (P2S-00023)"},
                            {"name": "representmentDate", "type": ["null", "string"], "default": null, "doc": "The date of the representation (P2S-00024)"},
                            {"name": "representmentReasonCode", "type": ["null", "string"], "default": null, "doc": "The reason for the representation (P2S-00025)"},
                            {"name": "representmentAmount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "The amount represented in the chargeback process (P2S-00026)"},
                            {"name": "arbitrationDate", "type": ["null", "string"], "default": null, "doc": "The date the arbitration process was initiated (P2S-00027)"},
                            {"name": "arbitrationReasonCode", "type": ["null", "string"], "default": null, "doc": "The reason code for the arbitration (P2S-00028)"},
                            {"name": "arbitrationAmount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "The amount being contested during the arbitration process (P2S-00029)"}
                        ]
                    }],
                    "default": null,
                    "doc": "Chargeback details if applicable"
                },
                {
                    "name": "clearing",
                    "type": {
                        "type": "record",
                        "name": "Clearing",
                        "doc": "Clearing information for the transaction",
                        "fields": [
                            {"name": "idPreClearing", "type": "string", "doc": "Unique identifier for the clearing entry (P2S-00030)"},
                            {"name": "settlementAmount", "type": "string", "doc": "The currency of the settlement amount (P2S-00035)"},
                            {"name": "dcc", "type": ["null", {
                                "type": "record",
                                "name": "DynamicCurrencyConversion",
                                "doc": "Dynamic Currency Conversion details",
                                "fields": [
                                    {"name": "isDcc", "type": "boolean", "doc": "Indicates if this is a DCC (Dynamic Currency Conversion) transaction (P2S-00038)"},
                                    {"name": "dccCurrency", "type": ["null", "string"], "default": null, "doc": "The DCC currency used in the transaction (P2S-00039)"},
                                    {"name": "dccMarkup", "type": ["null", "string"], "default": null, "doc": "The markup added to the DCC transaction (P2S-00040)"}
                                ]
                            }], "default": null, "doc": "Dynamic currency conversion details if applicable"}
                        ]
                    },
                    "doc": "Clearing details"
                },
                {
                    "name": "paymentInstrument",
                    "type": {
                        "type": "record",
                        "name": "PaymentInstrument",
                        "doc": "Information about the payment instrument used",
                        "fields": [
                            {"name": "paymentInstrumentId", "type": ["null", "string"], "default": null, "doc": "Unique identifier for the payment instrument (P2S-00041)"},
                            {"name": "maskedPan", "type": "string", "doc": "The masked (partially obfuscated) PAN of the card used for the transaction (P2S-00043)"},
                            {"name": "sequence", "type": ["null", "string"], "default": null, "doc": "Sequence number to identify the card usage instance (e.g., recurring payments) (P2S-00044)"},
                            {"name": "expiryDate", "type": ["null", "string"], "default": null, "doc": "The expiration date of the card (P2S-00045)"}
                        ]
                    },
                    "doc": "Payment instrument details"
                },
                {
                    "name": "reversals",
                    "type": ["null", {
                        "type": "record",
                        "name": "Reversals",
                        "doc": "Information about any reversals applied to the transaction",
                        "fields": [
                            {"name": "datetime", "type": ["null", "string"], "default": null, "doc": "Reversal datetime (P2S-00046)"},
                            {"name": "type", "type": ["null", "string"], "default": null, "doc": "Reversal type (Partial/Total) (P2S-00047)"},
                            {"name": "reason", "type": ["null", "string"], "default": null, "doc": "Reason for reversal (P2S-00048)"},
                            {"name": "amount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "Reversed amount (P2S-00049)"}
                        ]
                    }],
                    "default": null,
                    "doc": "Reversal details if applicable"
                },
                {
                    "name": "settlement",
                    "type": ["null", {
                        "type": "record",
                        "name": "Settlement",
                        "doc": "Information about the settlement of the transaction",
                        "fields": [
                            {"name": "settlementNetwork", "type": ["null", "string"], "default": null, "doc": "The payment network used in the settlement (e.g., Visa, Mastercard) (P2S-00050)"},
                            {"name": "settlementId", "type": ["null", "string"], "default": null, "doc": "The transaction ID used for settlement (P2S-00051)"}
                        ]
                    }],
                    "default": null,
                    "doc": "Settlement details if applicable"
                },
                {
                    "name": "transaction",
                    "type": {
                        "type": "record",
                        "name": "Transaction",
                        "doc": "Details of the transaction",
                        "fields": [
                            {"name": "type", "type": "string", "doc": "Transaction type (Purchase, Load, etc.) (P2S-00053)"},
                            {"name": "transactionExternalId", "type": "string", "doc": "Id sent by the processor (P2S-00056)"},
                            {"name": "transactionCountryCode", "type": "string", "doc": "Country code of the transaction (P2S-00057)"},
                            {"name": "transactionStatus", "type": "string", "doc": "Lifecycle of the transaction (P2S-00058)"},
                            {"name": "transactionAmount", "type": {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }, "doc": "Amount based on transaction status (P2S-00060)"},
                            {"name": "currency", "type": "string", "doc": "Currency of amount (ISO4217 or numeric) (P2S-00062)"},
                            {"name": "transactionAdditionalAmount", "type": {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }, "doc": "Additional Amount, Field 54 ISO8583 (P2S-00130)"}
                        ]
                    },
                    "doc": "Transaction details"
                },
                {
                    "name": "transactionRequest",
                    "type": {
                        "type": "record",
                        "name": "TransactionRequest",
                        "doc": "Information about the transaction request",
                        "fields": [
                            {"name": "xPayWalletAuthMethod", "type": ["null", "string"], "default": null, "doc": "Authentication method used in the wallet transaction (e.g., biometric, PIN) (P2S-00073)"},
                            {"name": "transactionRequestDatetime", "type": "string", "doc": "Acceptor local time (P2S-00074)"},
                            {"name": "transactionRequestBillingAmount", "type": {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }, "doc": "Amount billed in issuer currency (P2S-00075)"},
                            {"name": "transactionRequestBillingCurrency", "type": "string", "doc": "Billing currency (ISO4217) (P2S-00076)"},
                            {"name": "transactionRequestStan", "type": "string", "doc": "Systems Trace Audit Number (P2S-00077)"},
                            {"name": "transactionRequestRetrievalReferenceNumber", "type": "string", "doc": "Retrieval Reference Number (P2S-00078)"},
                            {"name": "transactionRequestAmount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "Requested amount in local currency (P2S-00079)"},
                            {"name": "transactionRequestCurrency", "type": ["null", "string"], "default": null, "doc": "Currency of requested amount (P2S-00080)"},
                            {"name": "transactionRequestTransmissionDatetime", "type": "string", "doc": "UTC transmission datetime (P2S-00081)"},
                            {"name": "transactionRequestBillingRate", "type": "string", "doc": "Conversion rate (P2S-00082)"},
                            {"name": "transactionRequestScheme", "type": "string", "doc": "Scheme processed the transaction (P2S-00083)"},
                            {"name": "recurringFlag", "type": ["null", "boolean"], "default": null, "doc": "Recurring payment flag (P2S-00084)"},
                            {"name": "technologyentryMode", "type": "string", "doc": "Technology entry mode (P2S-00085)"},
                            {"name": "technologyentryModemagneticStripe", "type": "boolean", "doc": "Indicates if the card was swiped (magnetic stripe entry) (P2S-00086)"},
                            {"name": "technologyentryModeEMVChip", "type": "boolean", "doc": "Indicates if the card was inserted for EMV chip-based processing (P2S-00087)"},
                            {"name": "technologyentryModeContactless", "type": "boolean", "doc": "Indicates if the card was used for a contactless transaction (NFC) (P2S-00088)"},
                            {"name": "technologyentryModemanualEntry", "type": "boolean", "doc": "Indicates if the card details were manually entered (P2S-00089)"},
                            {"name": "technologyentryModeFallback", "type": "boolean", "doc": "Indicates if fallback processing was used (in case of EMV failure) (P2S-00090)"},
                            {"name": "technologyentryModeNFC", "type": "boolean", "doc": "Indicates if NFC was used for the transaction (P2S-00091)"},
                            {"name": "technologyPOSConditionCode", "type": "string", "doc": "POS condition code (P2S-00092)"},
                            {"name": "technologyPOSConditionCodeCardPresent", "type": "boolean", "doc": "Indicates whether the card was physically present during the transaction (P2S-00093)"},
                            {"name": "technologyPOSConditionCodetransactionCondition", "type": ["null", "string"], "default": null, "doc": "Condition of the transaction (e.g., standard, refund) (P2S-00094)"},
                            {"name": "technologyPOSConditionCodePinVerification", "type": "boolean", "doc": "Indicates if PIN verification was performed (P2S-00095)"},
                            {"name": "technologyPOSConditionCodeSignatureAuthorization", "type": ["null", "boolean"], "default": null, "doc": "Indicates if signature authorization was used (P2S-00096)"},
                            {"name": "technologyPOSConditionCodeOnlineAuthorization", "type": "boolean", "doc": "Indicates whether online authorization was performed (P2S-00097)"},
                            {"name": "technologyPOSConditionCodeOfflineAuthorization", "type": ["null", "boolean"], "default": null, "doc": "Indicates whether offline authorization was used (P2S-00098)"},
                            {"name": "technologyPOSConditionCodeFaceToFace", "type": ["null", "boolean"], "default": null, "doc": "Indicates whether the transaction is face-to-face or remote (P2S-00099)"},
                            {"name": "technologyEMVtagsServiceCode", "type": ["null", "string"], "default": null, "doc": "Specifies the card service type (e.g., debit, credit) (P2S-00101)"},
                            {"name": "technologyEMVtagsTransactionSequenceCounter", "type": ["null", "string"], "default": null, "doc": "A unique sequence number for the transaction (P2S-00102)"},
                            {"name": "technologyEMVtagsTerminalType", "type": ["null", "string"], "default": null, "doc": "Type of terminal used to process the transaction (e.g., POS, ATM) (P2S-00103)"},
                            {"name": "technologyEMVtagsTerminalCapabilities", "type": ["null", "string"], "default": null, "doc": "Defines terminal capabilities (e.g., EMV, NFC) (P2S-00104)"},
                            {"name": "technologyEMVtagsIAC", "type": ["null", "string"], "default": null, "doc": "Issuer Action Code (indicates transaction outcome) (P2S-00105)"},
                            {"name": "technologyEMVtagsAC", "type": ["null", "string"], "default": null, "doc": "Cryptogram used in authorization (ARQC/TC/AAC) (P2S-00106)"},
                            {"name": "technologyEMVtagsCID", "type": ["null", "string"], "default": null, "doc": "Cryptogram Identifier, indicates the type of cryptogram (P2S-00107)"},
                            {"name": "technologyEMVtagsAIP", "type": ["null", "string"], "default": null, "doc": "Application Interchange Profile, defines card capabilities (P2S-00108)"},
                            {"name": "technologyEMVtagsIAD", "type": ["null", "string"], "default": null, "doc": "Issuer Application Data, used for issuer-specific risk management (P2S-00109)"},
                            {"name": "technology3DSecureAuthenticationIndicator", "type": ["null", "string"], "default": null, "doc": "Indicates the result of 3D Secure authentication (e.g., successful, failed) (P2S-00110)"},
                            {"name": "technology3DSecure3DSVersion", "type": ["null", "string"], "default": null, "doc": "Specifies the version of 3D Secure used (P2S-00111)"},
                            {"name": "technology3DSecureIssuerAuthenticationData", "type": ["null", "string"], "default": null, "doc": "Data used by the issuer for authentication (P2S-00112)"},
                            {"name": "technology3DSecureAuthenticationResponse", "type": ["null", "string"], "default": null, "doc": "Response from the issuer regarding authentication outcome (P2S-00113)"},
                            {"name": "technology3DSecureCAVV", "type": ["null", "string"], "default": null, "doc": "Cardholder Authentication Verification Value (used in 3D Secure) (P2S-00114)"},
                            {"name": "technology3DSecureDSTransID", "type": ["null", "string"], "default": null, "doc": "3D Secure 2 transaction identifier (P2S-00115)"},
                            {"name": "technology3DSecureDeviceChannel", "type": ["null", "string"], "default": null, "doc": "Indicates whether the transaction was made using a mobile device or desktop (P2S-00116)"},
                            {"name": "technology3DSecureAMID", "type": ["null", "string"], "default": null, "doc": "Authentication Method Indicator used in 3D Secure (e.g., biometric, PIN) (P2S-00117)"},
                            {"name": "technology3DSecureAAV", "type": ["null", "string"], "default": null, "doc": "Mastercard's equivalent of CAVV used in 3D Secure for cardholder authentication (P2S-00118)"},
                            {"name": "technology3DSecureECI", "type": ["null", "string"], "default": null, "doc": "Electronic Commerce Indicator used in 3D Secure transactions (P2S-00119)"},
                            {"name": "xPayWalletWalletID", "type": ["null", "string"], "default": null, "doc": "Unique identifier for the wallet user in the xPay system (P2S-00120)"},
                            {"name": "xPayWalletTokenizedPAN", "type": ["null", "string"], "default": null, "doc": "Token representing the actual PAN (Primary Account Number) for the transaction (P2S-00121)"},
                            {"name": "xPayWalletDeviceId", "type": ["null", "string"], "default": null, "doc": "Identifier for the mobile device used for the transaction (P2S-00122)"},
                            {"name": "xPayWalletTransactionToken", "type": ["null", "string"], "default": null, "doc": "One-time token used for this specific transaction (P2S-00123)"},
                            {"name": "xPayWalletWalletType", "type": ["null", "string"], "default": null, "doc": "Identifies the wallet provider (e.g., Apple Pay, Google Pay) (P2S-00124)"}
                        ]
                    },
                    "doc": "Transaction request details"
                },
                {
                    "name": "transactionResponse",
                    "type": {
                        "type": "record",
                        "name": "TransactionResponse",
                        "doc": "Information about the transaction response",
                        "fields": [
                            {"name": "code", "type": "string", "doc": "The approval code from the issuer (P2S-00125)"},
                            {"name": "amount", "type": ["null", {
                                "type": "bytes",
                                "logicalType": "decimal",
                                "precision": 20,
                                "scale": 10
                            }], "default": null, "doc": "The amount authorized by the issuer (P2S-00126)"},
                            {"name": "datetime", "type": ["null", "string"], "default": null, "doc": "Response timestamp from the issuer (P2S-00127)"},
                            {"name": "reason", "type": "string", "doc": "Reason for rejection if the transaction was declined (P2S-00128)"},
                            {"name": "authorizationId", "type": "string", "doc": "The authorization number returned by the issuer (P2S-00129)"}
                        ]
                    },
                    "doc": "Transaction response details"
                }
            ]
        }"""
        
        self.key_subject = f"{INPUT_TOPIC}-key"
        self.value_subject = f"{INPUT_TOPIC}-value"
        self.key_schema_id = register_schema(self.key_subject, self.key_schema_str)
        self.value_schema_id = register_schema(self.value_subject, self.value_schema_str)
        self.key_schema = parse_schema(self.key_schema_str)
        self.value_schema = parse_schema(self.value_schema_str)
        self.producer = Producer(producer_conf)
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([OUTPUT_TOPIC])
        self.consumed_messages = 0

    def avro_serialize(self, data, schema, schema_id):
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, data)
        raw_bytes = buf.getvalue()
        result = b"\0" + schema_id.to_bytes(4, byteorder="big") + raw_bytes
        return result

    def avro_deserialize(self, data, schema):
        if not data:
            raise ValueError("Empty message data")
        
        try:
            buf = io.BytesIO(data)
            # Check for Confluent wire format (magic byte + schema ID)
            if data[0] == 0:
                buf.read(1)  # Skip magic byte
                schema_id = int.from_bytes(buf.read(4), 'big')
            return fastavro.schemaless_reader(buf, schema)
        except Exception as e:
            print(f"Raw message (first 50 bytes): {data[:50]}")
            raise

    @task
    def produce_and_consume_with_stats(self):
        start_time = time.time()

        try:
            print("[Locust] Producing 100 messages...")

            # Produce 100 messages
            for i in range(100):
                key = {"id": f"id-{i}"}
                dec_val = decimal.Decimal("500.54")
                dec_bytes = decimal_to_bytes(dec_val)
                value = {
                    "acceptor": {
                        "id": "MERCH-5278",
                        "subId": "LYON-8921",
                        "terminalId": "TERM-69001-42",
                        "pricingRuleCode": "FR-GROCERY-PREMIUM",
                        "merchantCategoryCode": "5411",
                        "name": "Carrefour City Lyon Bellecour",
                        "city": "Lyon",
                        "country": "FR",
                        "identifier": "2206178452963817425309",
                        "referenceNumber": "REF-20250614-89245"
                    },
                    "acquirer": {
                        "acquiringInstitutionId": "19847",
                        "acquirerBin": "482963",
                        "country": "FR"
                    },
                    "card": {
                        "issuerBin": "497845"
                    },
                    "chargeback": {
                        "id": "CB-20250612847",
                        "originalId": "TR-20250610-45892",
                        "cardholderdisputedate": "2025-06-11T14:25:33Z",
                        "issuingDate": "2025-06-12T09:45:18Z",
                        "amount": dec_bytes,
                        "reasonCode": "4855",
                        "representment": "NO",
                        "representmentDate": "2025-06-13T16:22:07Z",
                        "representmentReasonCode": "2.1",
                        "representmentAmount": dec_bytes,
                        "arbitrationDate": "2025-06-14T11:15:44Z",
                        "arbitrationReasonCode": "B2",
                        "arbitrationAmount": dec_bytes
                    },
                    "clearing": {
                        "idPreClearing": "PCL-FR-20250614-652847",
                        "settlementAmount": "EUR",
                        "dcc": {
                            "isDcc": True,
                            "dccCurrency": "GBP",
                            "dccMarkup": "3.2"
                        }
                    },
                    "paymentInstrument": {
                        "paymentInstrumentId": "PI-FR-497845-7831",
                        "maskedPan": "497845******2194",
                        "sequence": "67",
                        "expiryDate": "2027-08"
                    },
                    "reversals": {
                        "datetime": "2025-06-14T09:18:52Z",
                        "type": "Full",
                        "reason": "Merchant Request",
                        "amount": dec_bytes
                    },
                    "settlement": {
                        "settlementNetwork": "VISA",
                        "settlementId": "MCARD-SET-FR-20250614-78451"
                    },
                    "transaction": {
                        "type": "Purchase",
                        "transactionExternalId": "FR-EXT-20250614-896324",
                        "transactionCountryCode": "FR",
                        "transactionStatus": "Approved",
                        "transactionAmount": dec_bytes,
                        "currency": "EUR",
                        "transactionAdditionalAmount": dec_bytes
                    },
                    "transactionRequest": {
                        "xPayWalletAuthMethod": "BIOMETRIC",
                        "transactionRequestDatetime": "2025-06-14T15:32:47+02:00",
                        "transactionRequestBillingAmount": dec_bytes,
                        "transactionRequestBillingCurrency": "GBP",
                        "transactionRequestStan": "456782",
                        "transactionRequestRetrievalReferenceNumber": "789542613984",
                        "transactionRequestAmount": dec_bytes,
                        "transactionRequestCurrency": "EUR",
                        "transactionRequestTransmissionDatetime": "2025-06-14T13:47:21Z",
                        "transactionRequestBillingRate": "0.86",
                        "transactionRequestScheme": "VISA",
                        "recurringFlag": False,
                        "technologyentryMode": "Contactless",
                        "technologyentryModemagneticStripe": False,
                        "technologyentryModeEMVChip": True,
                        "technologyentryModeContactless": True,
                        "technologyentryModemanualEntry": False,
                        "technologyentryModeFallback": False,
                        "technologyentryModeNFC": True,
                        "technologyPOSConditionCode": "59",
                        "technologyPOSConditionCodeCardPresent": True,
                        "technologyPOSConditionCodetransactionCondition": "Normal",
                        "technologyPOSConditionCodePinVerification": True,
                        "technologyPOSConditionCodeSignatureAuthorization": False,
                        "technologyPOSConditionCodeOnlineAuthorization": True,
                        "technologyPOSConditionCodeOfflineAuthorization": False,
                        "technologyPOSConditionCodeFaceToFace": True,
                        "technologyEMVtagsServiceCode": "201",
                        "technologyEMVtagsTransactionSequenceCounter": "2847591",
                        "technologyEMVtagsTerminalType": "22",
                        "technologyEMVtagsTerminalCapabilities": "E0F8C8",
                        "technologyEMVtagsIAC": "DC4004A800",
                        "technologyEMVtagsAC": "8010F22A0E3668BC",
                        "technologyEMVtagsCID": "80",
                        "technologyEMVtagsAIP": "3F00",
                        "technologyEMVtagsIAD": "06010A03600000",
                        "technology3DSecureAuthenticationIndicator": "Y",
                        "technology3DSecure3DSVersion": "2.2.0",
                        "technology3DSecureIssuerAuthenticationData": "IAD-FR-20250614-789456",
                        "technology3DSecureAuthenticationResponse": "Y",
                        "technology3DSecureCAVV": "kAjdh7CIuG3m8wXdp5ZRTmnHGkwPCzyr",
                        "technology3DSecureDSTransID": "b8745c92-d8e3-4fa9-bc72-1e489bcd74f6",
                        "technology3DSecureDeviceChannel": "APP",
                        "technology3DSecureAMID": "73",
                        "technology3DSecureAAV": "s9vdgFnK421==QzYxgh7SlmR8v2=",
                        "technology3DSecureECI": "05",
                        "xPayWalletWalletID": "APPLEPAY-FR-USER-789456",
                        "xPayWalletTokenizedPAN": "xf4dlpMnvcbp7GnnVjxd9Ww+kL8vq/pq",
                        "xPayWalletDeviceId": "FR-IPHONE-15-PRO-847592136",
                        "xPayWalletTransactionToken": "tk_fr_20250614_847592_bnclm8",
                        "xPayWalletWalletType": "APPLEPAY"
                    },
                    "transactionResponse": {
                        "code": "000",
                        "amount": dec_bytes,
                        "datetime": "2025-06-14T15:33:12Z",
                        "reason": "APPROVED",
                        "authorizationId": "AUTH-FR-20250614-56789"
                    }
                }
                
                key_bytes = self.avro_serialize(key, self.key_schema, self.key_schema_id)
                value_bytes = self.avro_serialize(value, self.value_schema, self.value_schema_id)
                self.producer.produce(INPUT_TOPIC, key=key_bytes, value=value_bytes)

            self.producer.flush()
            produce_end_time = time.time()

            print("[Locust] Consuming 20 processed messages...")

            # Consume 20 processed messages
            messages = []
            consume_start = time.time()

            while len(messages) < 20 and (time.time() - start_time) < 10:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
            
                try:
                    value_data = msg.value()[5:]  # Skip first 5 bytes (magic byte + schema ID)
                    value = self.avro_deserialize(value_data, self.value_schema)
                    messages.append(value)
                    self.consumed_messages += 1
                except Exception as e:
                    print(f"Failed to deserialize message: {e}")

            # Validation
            if len(messages) != 20:
                raise Exception(f"Expected 20 messages, got {len(messages)}")

            # Calculate timings
            total_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            produce_time = (produce_end_time - start_time) * 1000
            consume_time = (time.time() - consume_start) * 1000

            print(f"[Locust] Task complete: 20 processed messages verified.")
            print(f"[Locust] Timings - Total: {total_time:.2f}ms, Produce: {produce_time:.2f}ms, Consume: {consume_time:.2f}ms")

            # Fire success event
            events.request.fire(
                request_type="kafka_worker",
                name="produce_and_consume_100_20",
                response_time=total_time,
                response_length=120,  # 100 produced + 20 consumed
                context={},
                exception=None
            )

        except Exception as e:
            # Fire failure event
            events.request.fire(
                request_type="kafka_worker",
                name="produce_and_consume_100_20",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                context={},
                exception=e
            )
            raise

    def on_stop(self):
        self.consumer.close()
        self.producer.flush()
        print("[Locust] KafkaUser stopped and resources cleaned up.")