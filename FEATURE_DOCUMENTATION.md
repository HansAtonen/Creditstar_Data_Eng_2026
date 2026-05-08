# Data Science Streaming Pipeline - Feature Documentation

## Overview
This document describes the implementation of three client-level features in the streaming pipeline for the credit data engineering task.

## Features Implemented

### 1. client.paid_loans.count
**Description**: Number of previously paid loans for each client

**Calculation Logic**:
```
COUNT(loans WHERE status = 'paid' AND client_id = X)
```

**Implementation**:
- Monitors Kafka topic `credit.public.loan` for CDC events.
- Maintains an in-memory store of all loan statuses.
- Filters loans by `client_id` and counts those with `status = 'paid'`.
- Returns count (0 if no paid loans).

**Edge Cases & Assumptions**:
- **No paid loans**: Returns 0 (explicit count of zero paid loans).
- **Null status values**: Treated as not 'paid', excluded from count.
- **Multiple loan statuses**: Only loans with exact match 'paid' are counted.

---

### 2. client.days_since_last_late_payment
**Description**: Number of days since the last late payment made by a client

**Calculation Logic**:
```
TODAY - MAX(payment.created_on WHERE payment.status = 'late' AND loan.client_id = X)
```

**Implementation**:
- Monitors Kafka topic `credit.public.payment`.
- Resolves `client_id` by cross-referencing the `loan_id` in the `loan_store`.
- Filters payments with `status = 'late'` and identifies the most recent date.
- Calculates days between current system time and that payment date.
- Returns null if no late payments exist.

**Edge Cases & Assumptions**:
- **No late payments**: Returns NULL/None (client has no history of late payments).
- **Multiple late payments**: Uses the most recent one based on `created_on`.
- **Today's late payment**: Returns 0 days.
- **Timezone**: Uses system date (no timezone adjustments).

---

### 3. client.profit_in_last_90_days.rate
**Description**: Sum of interest payments received from loans issued in the last 90 days divided by the sum of loan amounts issued in the last 90 days

**Calculation Logic**:
```
SUM(payment.interest WHERE loan.created_on >= TODAY - 90 days AND loan.client_id = X) / 
SUM(loan.amount WHERE loan.created_on >= TODAY - 90 days AND loan.client_id = X)
```

**Implementation**:
- Filters the `loan_store` for loans created within the last 90 days for specific `client_id`.
- Sums the `interest` from all payments linked specifically to those recent loans.
- Divides the interest sum by the total principal `amount` of those loans.
- Returns null if no loans were issued in the 90-day window.

**Edge Cases & Assumptions**:
- **No loans in last 90 days**: Returns NULL (prevents division by zero).
- **Loans exist but no payments**: Returns 0.0 (zero interest received).
- **Date boundary**: 90-day window is calculated from `pd.Timestamp.now()`.
- **Partial payments**: All interest recorded in the payment table for qualifying loans is included.

---

## Data Sources

### Loan Table
- `id`: Loan identifier
- `client_id`: Client identifier (foreign key)
- `amount`: Loan amount
- `created_on`: Loan creation date
- `status`: Loan status (e.g., 'paid', 'active', 'late')

### Payment Table
- `id`: Payment identifier
- `loan_id`: Loan identifier (foreign key)
- `amount`: Payment amount
- `interest`: Interest portion of payment
- `principle`: Principal portion of payment
- `status`: Payment status (e.g., 'late', 'on-time')
- `created_on`: Payment date

---

## Implementation Notes

1. **Pure Streaming**: Features are re-computed and uploaded immediately upon every database event to ensure <1s data availability for decision services.
2. **Materialized State**: Uses in-memory Python dictionaries (`loan_store`, `payment_store`) to allow for rapid cross-table lookups without querying Postgres.
3. **Change Data Capture (CDC)**: Utilizes Debezium operation codes (`op`) to handle inserts, updates, and deletes effectively within the feature store.
4. **Partitioning Strategy**: Files are stored in MinIO using Hive-style partitioning (`client_id={id}/`) to optimize historical data retrieval for ML training.

---

## Output Format

Features are streamed to MinIO object storage as Parquet files.
**Path Strategy**: `client_id={id}/{timestamp}.parquet`
**Columns**:
- `client_id`: Client identifier
- `paid_loans_count`: Count of paid loans (integer)
- `days_since_last_late_payment`: Days since late payment (integer or null)
- `profit_in_last_90_days_rate`: Profit rate (float or null)
- `computed_at`: ISO timestamp of the feature calculation
