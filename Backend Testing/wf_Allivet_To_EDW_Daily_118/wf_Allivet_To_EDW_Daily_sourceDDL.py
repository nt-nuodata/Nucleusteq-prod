# Databricks notebook source
# COMMAND ----------

CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.Allivet_Daily_Invoice(InvoicePostingDate STRING,
InvoiceCode STRING,
TransactionType STRING,
Allivet_Order STRING,
PetSmart_Order STRING,
AllivetSku STRING,
PetSmartSku BIGINT,
UPC BIGINT,
UnitsSold BIGINT,
ProductCost BIGINT,
RetailPrice BIGINT,
Manufacturer STRING,
Brand STRING,
Title STRING,
FreightFee BIGINT,
PackagingFee BIGINT,
DispensingFee BIGINT,
shipped_on STRING,
fulfillment_origin_zip_code STRING,
shipping_carrier STRING,
TrackingNumber STRING,
Customer_id STRING,
Pet_name STRING,
Pet_type STRING,
Breed_type STRING,
Pet_gender STRING,
Pet_is_pregnant STRING,
pet_DOB STRING,
Pet_weight STRING,
Pet_allergy STRING,
Pet_medical_conditions STRING,
Vet_clinic_name STRING,
Veterinary_name STRING,
Vet_address STRING,
Vet_city STRING,
Vet_state STRING,
Vet_zip STRING,
Vet_phone_number STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.Allivet_Daily_Order(AllivetOrderCode STRING,
PetsmartOrderCode STRING,
AllivetOrderDate STRING,
PetsmartOrderDate STRING,
Subtotal BIGINT,
Freight BIGINT,
Total BIGINT,
ShippingMethodCode STRING,
OrderStatus STRING,
IsVoided STRING,
IsOnHold STRING,
SalesOrderDateCreated STRING,
SalesOrderDateModified STRING,
DateShipped STRING,
IsShipped STRING,
InternalNotes STRING,
PublicNotes STRING,
AutoShip_DiscountAmount BIGINT,
OrderMerchantNotes STRING,
IsRiskOrder STRING,
RiskReasons STRING,
OriginalShippingMethodCode STRING,
IsShipHold STRING,
DateShipHold STRING,
DateShipReleased STRING,
AllivetSku STRING,
PetsmartSku STRING,
LineNum BIGINT,
QuantityOrdered BIGINT,
ItemDescription STRING,
ExtPrice BIGINT,
CustomerSalesOrderDetailDateCreated STRING,
CustomerSalesOrderDetailDateModified STRING,
HowdowegetyoutRx STRING,
VetCode BIGINT,
PetCode BIGINT,
IsOnHold1 STRING,
IsOnHoldToFill_C STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SKU_PROFILE(PRODUCT_ID INT,
SKU_NBR INT,
SKU_TYPE STRING,
PRIMARY_UPC_ID BIGINT,
STATUS_ID STRING,
SUBS_HIST_FLAG STRING,
SUBS_CURR_FLAG STRING,
SKU_DESC STRING,
ALT_DESC STRING,
SAP_CATEGORY_ID INT,
SAP_CLASS_ID INT,
SAP_DEPT_ID INT,
SAP_DIVISION_ID INT,
PRIMARY_VENDOR_ID BIGINT,
PARENT_VENDOR_ID BIGINT,
COUNTRY_CD STRING,
IMPORT_FLAG STRING,
HTS_CODE_ID BIGINT,
CONTENTS INT,
CONTENTS_UNITS STRING,
WEIGHT_NET_AMT INT,
WEIGHT_UOM_CD STRING,
SIZE_DESC STRING,
BUM_QTY FLOAT,
UOM_CD STRING,
UNIT_NUMERATOR FLOAT,
UNIT_DENOMINATOR FLOAT,
BUYER_ID STRING,
PURCH_GROUP_ID INT,
PURCH_COST_AMT INT,
NAT_PRICE_US_AMT INT,
TAX_CLASS_ID STRING,
VALUATION_CLASS_CD STRING,
BRAND_CD STRING,
BRAND_CLASSIFICATION_ID INT,
OWNBRAND_FLAG STRING,
STATELINE_FLAG STRING,
SIGN_TYPE_CD STRING,
OLD_ARTICLE_NBR STRING,
VENDOR_ARTICLE_NBR STRING,
INIT_MKDN_DT TIMESTAMP,
DISC_START_DT TIMESTAMP,
ADD_DT TIMESTAMP,
DELETE_DT TIMESTAMP,
UPDATE_DT TIMESTAMP,
FIRST_SALE_DT TIMESTAMP,
LAST_SALE_DT TIMESTAMP,
FIRST_INV_DT TIMESTAMP,
LAST_INV_DT TIMESTAMP,
LOAD_DT TIMESTAMP,
BASE_NBR STRING,
BP_COLOR_ID STRING,
BP_SIZE_ID STRING,
BP_BREED_ID STRING,
BP_ITEM_CONCATENATED STRING,
BP_AEROSOL_FLAG INT,
BP_HAZMAT_FLAG INT,
CANADIAN_HTS_CD STRING,
NAT_PRICE_CA_AMT INT,
NAT_PRICE_PR_AMT INT,
RTV_DEPT_CD STRING,
GL_ACCT_NBR INT,
ARTICLE_CATEGORY_ID INT,
COMPONENT_FLAG STRING,
ZDISCO_SCHED_TYPE_ID STRING,
ZDISCO_MKDN_SCHED_ID STRING,
ZDISCO_PID_DT TIMESTAMP,
ZDISCO_START_DT TIMESTAMP,
ZDISCO_INIT_MKDN_DT TIMESTAMP,
ZDISCO_DC_DT TIMESTAMP,
ZDISCO_STR_DT TIMESTAMP,
ZDISCO_STR_OWNRSHP_DT TIMESTAMP,
ZDISCO_STR_WRT_OFF_DT TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_DAILY_INVOICE_PRE(INVOICE_POSTING_DATE_v2 DATE,
INVOICE_CODE STRING,
TRANSACTION_TYPE STRING,
ALLIVET_ORDER_CODE STRING,
ALLIVET_SKU STRING,
PETSMART_ORDER_CODE STRING,
PETSMART_SKU INT,
UPC BIGINT,
UNITS_SOLD INT,
PRODUCT_COST INT,
RETAIL_PRICE INT,
MANUFACTURER STRING,
BRAND STRING,
TITLE STRING,
FREIGHT_FEE INT,
PACKAGING_FEE INT,
DISPENSING_FEE INT,
SHIPPED_DATE DATE,
FULFILLMENT_ORIGIN_ZIP_CODE STRING,
SHIPPING_CARRIER STRING,
TRACKING_NUMBER STRING,
ALLIVET_CUSTOMER_ID STRING,
PET_NAME STRING,
PET_TYPE STRING,
BREED_TYPE STRING,
PET_GENDER STRING,
PET_DOB DATE,
PET_WEIGHT INT,
PET_ALLERGY STRING,
PET_MEDICAL_CONDITIONS STRING,
PET_IS_PREGNANT INT,
VET_CLINIC_NAME STRING,
VETERINARY_NAME STRING,
VET_ADDRESS STRING,
VET_CITY STRING,
VET_STATE STRING,
VET_ZIP STRING,
VET_PHONE_NUMBER STRING,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_INVOICE_DAY(INVOICE_POSTING_DT DATE,
INVOICE_CD STRING,
TXN_TYPE STRING,
ALLIVET_ORDER_NBR STRING,
ALLIVET_SKU_NBR STRING,
PRODUCT_ID INT,
PETSMART_ORDER_NBR STRING,
PETSMART_SKU_NBR INT,
UPC_ID BIGINT,
SOLD_UNITS_QTY INT,
PRODUCT_COST INT,
RETAIL_PRICE INT,
MANUFACTURER STRING,
BRAND_NAME STRING,
TITLE STRING,
FREIGHT_FEE_AMT INT,
PACKAGING_FEE_AMT INT,
DISPENSING_FEE_AMT INT,
SHIPPED_DT DATE,
FULFILLMENT_ORIGIN_ZIP_CD STRING,
SHIP_CARRIER_NAME STRING,
TRACKING_NBR STRING,
ALLIVET_CUSTOMER_NBR STRING,
PET_NAME STRING,
PET_TYPE STRING,
BREED_TYPE STRING,
PET_GENDER STRING,
PET_BIRT_DT DATE,
PET_WEIGHT INT,
PET_ALLERGY_DESC STRING,
PET_MEDICAL_CONDITION STRING,
PET_PREGNANT_FLAG INT,
VET_CLINIC_NAME STRING,
VET_NAME STRING,
VET_ADDRESS STRING,
VET_CITY STRING,
VET_STATE_CD STRING,
VET_ZIP_CD STRING,
VET_PHONE_NBR STRING,
DELETE_FLAG INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.Allivet_Daily_System_Audit(Operation STRING,
CustomerCode STRING,
ExtraInfo STRING,
ChangeDate STRING,
FromItemCode STRING,
ToItemCode STRING,
AllivetOrderCode STRING,
LineNum BIGINT,
PrescriptionID STRING,
Counter BIGINT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_DAILY_SYSTEM_AUDIT_PRE_v2(ALLIVET_ORDER_CODE_v2 STRING,
ALLIVET_ORDER_COUNTER INT,
ALLIVET_ORDER_LINE_NUMBER INT,
OPERATION STRING,
CUSTOMER_CODE STRING,
EXTRA_INFO STRING,
CHANGE_DATE DATE,
FROM_ITEM_CODE STRING,
TO_ITEM_CODE STRING,
PRESCRIPTION_ID BIGINT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_SYSTEM_AUDIT_DAY(ALLIVET_ORDER_NBR STRING,
ALLIVET_ORDER_CNTR INT,
ALLIVET_ORDER_LN_NBR INT,
OPERATION STRING,
ALLIVET_CUSTOMER_NBR STRING,
EXTRA_INFO STRING,
ORDER_CHG_DT DATE,
FROM_ITEM_CD STRING,
TO_ITEM_CD STRING,
PRESCRIPTION_ID BIGINT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_DAILY_ORDER_PRE(ALLIVET_ORDER_CODE STRING,
ALLIVET_ORDER_LINE_NUMBER INT,
ALLIVET_ORDER_DATE DATE,
PETSMART_ORDER_DATE DATE,
ORDER_STATUS STRING,
PETSMART_ORDER_CODE STRING,
SUB_TOTAL INT,
FREIGHT INT,
TOTAL INT,
SHIPPING_METHOD_CODE STRING,
IS_ORDER_VOIDED INT,
IS_ORDER_ONHOLD INT,
ORDER_CREATED_DATE DATE,
ORDER_MODIFIED_DATE DATE,
SHIPPED_DATE DATE,
IS_ORDER_SHIPPED INT,
INTERNAL_NOTES STRING,
PUBLIC_NOTES STRING,
AUTOSHIP_DISCOUNT_AMOUNT INT,
ORDER_MERCHANT_NOTES STRING,
IS_RISKORDER INT,
RISK_REASONS STRING,
ORIGINAL_SHIPPING_METHOD_CODE STRING,
IS_SHIPHOLD INT,
SHIPHOLD_DATE DATE,
SHIPRELEASED_DATE DATE,
ALLIVET_SKU STRING,
PETSMART_SKU INT,
ORDERED_QUANTITY INT,
ITEM_DESCRIPTION STRING,
EXT_PRICE INT,
ORDER_DETAIL_CREATED_DATE DATE,
ORDER_DETAIL_MODIFIED_DATE DATE,
HOW_TO_GET_RX STRING,
VET_CODE INT,
PET_CODE INT,
IS_ORDER_DETAIL_ONHOLD INT,
IS_ONHOLD_TOFILL INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_ORDER_DAY(ALLIVET_ORDER_NBR STRING,
ALLIVET_ORDER_LN_NBR INT,
ALLIVET_ORDER_DT DATE,
PETSMART_ORDER_DT DATE,
ORDER_STATUS STRING,
PRODUCT_ID INT,
PETSMART_ORDER_NBR STRING,
PETSMART_SKU_NBR INT,
ALLIVET_SKU_NBR STRING,
SUB_TOTAL_AMT INT,
FREIGHT_COST INT,
TOTAL_AMT INT,
SHIP_METHOD_CD STRING,
ORDER_VOIDED_FLAG INT,
ORDER_ONHOLD_FLAG INT,
ORDER_CREATED_DT DATE,
ORDER_MODIFIED_DT DATE,
SHIPPED_DT DATE,
ORDER_SHIPPED_FLAG INT,
INTERNAL_NOTES STRING,
PUBLIC_NOTES STRING,
AUTOSHIP_DISCOUNT_AMT INT,
ORDER_MERCHANT_NOTES STRING,
RISKORDER_FLAG INT,
RISK_REASON STRING,
ORIG_SHIP_METHOD_CD STRING,
SHIP_HOLD_FLAG INT,
SHIP_HOLD_DT DATE,
SHIP_RELEASE_DT DATE,
ORDER_QTY INT,
ITEM_DESC STRING,
EXT_PRICE INT,
ORDER_DETAIL_CREATED_DT DATE,
ORDER_DETAIL_MODIFIED_DT DATE,
HOW_TO_GET_RX STRING,
VET_CD INT,
PET_CD INT,
ORDER_DETAIL_ONHOLD_FLAG INT,
ONHOLD_TO_FILL_FLAG INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.ALLIVET_ORDER_STATUS_DAY(ALLIVET_ORDER_NBR STRING,
RX_HOLD_DT DATE,
OPEN_DT DATE,
COMPLETE_DT DATE,
VOID_DT DATE,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;