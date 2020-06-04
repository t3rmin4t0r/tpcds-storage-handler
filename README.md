# tpcds-storage-handler
A Storage handler for TPC-DS

## BUILDING

    git submodule update --init --recursive
    mvn clean package

## Add the jar
    the relevant jar is in */*/hive-tpcds-handler-4.0.0-SNAPSHOT.jar
    
    Add jar to current session
    
    ```
    CREATE TEMPORARY EXTERNAL  TABLE inventory(inv_date_sk bigint, inv_item_sk bigint, inv_warehouse_sk bigint, inv_quantity_on_hand int) STORED BY 'org.notmysock.benchmark.tpcds.TpcdsHandler' TBLPROPERTIES('tpcds.table'='inventory', 'tpcds.scale'='10');
    ```
    
    Verify by doing
    
    ```
    select * from inventory limit 10;
    ```
