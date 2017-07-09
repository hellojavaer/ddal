# DDAL

DDAL(Distributed Data Access Layer) is a simple solution to access database shard.


## License

DDAL is licensed under **Apache Software License, Version 2.0**.


## Summary

DDAL include ddal-ddr, ddal-sequence, ddal-spring and ddal-jsqlparser.
DDR's core module is sql routing which corresponds to ShardParser and datasource routing which corresponds to DataSourceManager. sql routing is actually converting the logic table name and schema name to its physical name in sql. ShardRouteRuleBinding determines which schema-table should be converted, ShardRouteRule and ShardRouteContext determines how to converte the logic table name and schema name.(see more detail aboute this).
sql routing result contains converted sql and the schemas which associated witch the converted tables.
In DataSourceManager, one datasource is associated with multiple schemas, DataSourceManager depends on the inputing schemas to choose which datasource should be returned.
After sql routing, datasource routing will use the schemas returned by sql routing to choose which datasource will be returned. ReadWriteDataSourceManager requires at least one schema to choose datasource from confirmations and SingleDataSourceManager doesn't have this limiting. Meanwhile if you are trying to use two schemas which are not contained in the same datasource(Note! the same datasource doesn't mean the physical concept but a logic concept),'CrossingDataSourceException' will be thrown. In a transaction, only one datasource can be bound in one transaction, If you need to access two datasources in a invocation you can create a new transaction for the second datasource.


## News
- version 1.0.0.M3 released.
- version 1.0.0.M2 released.
- version 1.0.0.M1 released.

### [Release Notes](https://github.com/hellojavaer/ddal/releases)

## Extensions in the latest version 1.0.0.M3

- provide a easier way for scanning table query

```
// list all physical tables by specified 'scName' and 'tbName'
List<RouteInfo> routeInfos = shardRouter.getRouteInfos(scName, tbName);for (RouteInfo routeInfo : routeInfos) {
    ShardRouteContext.setRouteInfo(scName, tbName, routeInfo);
    // TODO: do your business here
    // ...
    ShardRouteContext.clearContext();
}
```

## Extensions in the version 1.0.0.M2

- provide a more simple way to configure route rule

```
SpelShardRouteRule rule = new SpelShardRouteRule();
rule.setScRouteRule("{#scName}_{#format('%02d', #sdValue % 4)}");
rule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue % 8)}");
```

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

<!-- use annotation to route in spring environment -->
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-spring</artifactId>
    <version>x.x.x</version>
</dependency>
<!-- use sequence service -->
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-sequence</artifactId>
    <version>x.x.x</version>
</dependency>

```
