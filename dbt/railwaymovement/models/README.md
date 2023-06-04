# Data Model in dbt

This directory contains the data model transform from the source data using SQL via dbt framework.

The `.sql` files are the actual data model implementation. Each one represent each data model. 

The `_src.yml` file is the data source definition, explicitly tell where the data source is.

The `_model.yml` file is the data model documentation, it explicitly documents the metadata and detail about the data model, where the developers need to write by their own. It is optional, and they could partially fill the documentation. But full document for the data model would be more encouraged.

## Data Source Definition

The following section explain the essential components in `_src.yml` file.

1. The `name, schema, database` directly under `sources` is describe where the actual database project and correspond schema is. 
    The `name` is represent as alias for that data source instance.  
    In this project, the source is BigQuery data warehouse, so the `database` is the whole project id, and the `schema` is the dataset within the BigQuery project. 
2. Under `tables` section define the table description within the data source.
    The `name` is the actual table name in the data source.
    The `description` is just for explain that specific table.
    The `columns` specifies columns presented in the table
    The `freshness` define the freshness of the data, the `count` means the unit number of time and correspond to the `period` time unit.
    The `loaded_at_field` requires for determine the column to calculate in the `freshness`

```yml
version: 2

sources:
  - name: railwaymovement
    schema: prac_dbt_end_proj
    database: data-engineer-bootcamp-384606

    tables:
      - name: movement
        description: The movements of all train on the networkrailway data.
        columns:
          - name: event_type
            description: Event type
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 2, period: hour}
        loaded_at_field: actual_timestamp
```
