# DDAL

DDAL(Distributed Data Access Layer) is a simple solution for accessing database shard.


## License

DDAL is licensed under **Apache Software License, Version 2.0**.


## Summary

DDAL contains ddal-ddr, ddal-sequence, ddal-spring and ddal-jsqlparser.
DDR's core modules are sql routing which corresponds to ShardParser and datasource routing which corresponds to DataSourceManager. sql routing is actually converting the logic table name and schema name to its physical name in sql. ShardRouteRuleBinding determines which schema-table should be converted, ShardRouteRule and ShardRouteContext determines how to converte the logic table name and schema name.(see more detail aboute this).
sql routing result contains converted sql and the schemas which associated witch the converted tables.
In DataSourceManager, one datasource is associated with multiple schemas, DataSourceManager depends on the inputing schemas to choose which datasource should be returned.
After sql routing, datasource routing will use the schemas returned by sql routing to choose which datasource will be returned. ReadWriteDataSourceManager requires at least one schema to choose datasource from confirmations and SingleDataSourceManager doesn't have this limiting. Meanwhile if you are trying to use two schemas which are not contained in the same datasource(Note! the same datasource doesn't mean the physical concept but a logic concept),'CrossingDataSourceException' will be thrown. In a transaction, only one datasource can be bound in one transaction, If you need to access two datasources in a invocation you can create a new transaction for the second datasource.


## Documentation

- [Documentation Home](https://github.com/hellojavaer/ddal/wiki)
- [Frequently Asked Questions](https://github.com/hellojavaer/ddal/wiki/faq)


## Download

http://repo1.maven.org/maven2/org/hellojavaer/ddal/

## Maven

```xml
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-ddr</artifactId>
    <version>x.x.x</version>
</dependency>
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-jsqlparser</artifactId>
    <version>x.x.x</version>
</dependency>

<!-- to use annotation to route in spring environment -->
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-spring</artifactId>
    <version>x.x.x</version>
</dependency>
<!-- to use sequence service -->
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-sequence</artifactId>
    <version>x.x.x</version>
</dependency>

```
