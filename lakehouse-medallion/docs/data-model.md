# Data Model

## Silver — 3NF, conformed

```mermaid
erDiagram
    CUSTOMER ||--o{ ORDER : places
    ORDER ||--|{ ORDER_LINE : contains
    PRODUCT ||--o{ ORDER_LINE : appears_in
    STORE ||--o{ ORDER : fulfilled_at

    CUSTOMER {
      string customer_id PK
      string email
      string country
      timestamp valid_from
      timestamp valid_to
      boolean is_current
    }
    ORDER {
      string order_id PK
      string customer_id FK
      timestamp order_ts
      string status
      decimal total
    }
    ORDER_LINE {
      string order_id PK
      string line_id PK
      string sku FK
      int qty
      decimal unit_price
    }
    PRODUCT {
      string sku PK
      string category
      string brand
      timestamp valid_from
      timestamp valid_to
      boolean is_current
    }
```

## Gold — star schema

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ FACT_SALES_DAILY : "buys"
    DIM_PRODUCT  ||--o{ FACT_SALES_DAILY : "sold"
    DIM_DATE     ||--o{ FACT_SALES_DAILY : "on"
    DIM_STORE    ||--o{ FACT_SALES_DAILY : "at"

    FACT_SALES_DAILY {
      date order_date FK
      bigint dim_customer_sk FK
      bigint dim_product_sk FK
      bigint dim_store_sk FK
      string channel
      int units
      decimal gross_revenue
      int orders
    }
    DIM_CUSTOMER {
      bigint dim_customer_sk PK
      string customer_id
      string customer_segment
      string country
    }
    DIM_PRODUCT {
      bigint dim_product_sk PK
      string sku
      string category
      string brand
    }
    DIM_DATE {
      date date_key PK
      int day_of_week
      int fiscal_period
    }
    DIM_STORE {
      bigint dim_store_sk PK
      string store_id
      string region
    }
```

## SCD Type 2 columns

All dimensions carry these audit columns:

| Column | Purpose |
|---|---|
| `dim_xxx_sk` | Surrogate key, monotonically increasing |
| `valid_from` | Timestamp when this version became active |
| `valid_to` | Timestamp when this version was superseded (NULL if current) |
| `is_current` | Boolean flag for fast filtering of latest version |
| `_attr_hash` | SHA-256 over tracked attributes, used for change detection |
