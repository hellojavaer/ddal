# DDAL

[![Build Status](https://travis-ci.org/hellojavaer/ddal.svg?branch=master)](https://travis-ci.org/hellojavaer/ddal)
[![GitHub release](https://img.shields.io/github/release/hellojavaer/ddal.svg)](https://github.com/hellojavaer/ddal/releases)

DDAL(Distributed Data Access Layer) is a simple solution to access database shard.


## License

DDAL is dual licensed under **LGPL V2.1** and **Apache Software License, Version 2.0**.

### [Release Notes](https://github.com/hellojavaer/ddal/releases)

## Extensions in the latest version 1.0.0.M5

- optimize route rule expression parser

```
// old
SpelShardRouteRule rule = new SpelShardRouteRule();
rule.setScRouteRule("{#scName}_{#format('%02d', #sdValue % 4)}");
rule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue % 8)}");

// new
SpelShardRouteRule rule = new SpelShardRouteRule();
rule.setScRouteRule("{scName}_{format('%02d', sdValue % 4)}");
rule.setTbRouteRule("{tbName}_{format('%04d', sdValue % 8)}");
```

- optimize range expression parser

```
"1,2,3"  => 1,2,3
"[1..3]" => 1,2,3
"['A'..'C','X']" => A,B,C,X
"[0..1][0..1]" => 00,01,10,11
"Hi![' Allen',' Bob']" => Hi! Allen,Hi! Bob
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

## Qick start

See [ddal-demos](https://github.com/hellojavaer/ddal-demos/)
```
// TODO: define raw datasource
DataSource wds00 = null;
DataSource rds00 = null;

// 1. Define route rule
SpelShardRouteRule routeRule = new SpelShardRouteRule();
routeRule.setScRouteRule("{scName}_{format('%02d', sdValue % 2)}");
routeRule.setTbRouteRule("{tbName}_{format('%04d', sdValue % 8)}");

// 2. Route rules bind to logical table name
List<SimpleShardRouteRuleBinding> ruleBindings = new ArrayList<>();
ruleBindings.add(new SimpleShardRouteRuleBinding("schema_name", "table_name", "id", routeRule, "[0..7]"));
// TODO: add more route-rule-binding here ...
SimpleShardRouter shardRouter = new SimpleShardRouter(ruleBindings);

// 3. Schema names bind to datasource
List<WeightedDataSource> weightedDataSources = new ArrayList<>();
weightedDataSources.add(new WeightedDataSource(rds00, 9));
weightedDataSources.add(new WeightedDataSource(wds00, 1));
// TODO: add more datasource here ...
List<ReadOnlyDataSourceBinding> readOnlyDataSourceBindings = new ArrayList<>();
readOnlyDataSourceBindings.add(new ReadOnlyDataSourceBinding("schema_name_0[0..1]", weightedDataSources));
List<WriteOnlyDataSourceBinding> writeOnlyDataSourceBindings = new ArrayList<>();
writeOnlyDataSourceBindings.add(new WriteOnlyDataSourceBinding("schema_name_0[0..1]", (DataSource) Arrays.asList(wds00)));
// TODO: add more datasource here ...
DefaultReadWriteDataSourceManager dataSourceManager = new DefaultReadWriteDataSourceManager();
dataSourceManager.setShardRouter(shardRouter);
dataSourceManager.setReadOnlyDataSources(readOnlyDataSourceBindings);
dataSourceManager.setWriteOnlyDataSources(writeOnlyDataSourceBindings);

// 4. Define a 'DDRDataSource' to proxy the raw datasources
DataSource ddrDataSource = new DefaultDDRDataSource(dataSourceManager, new SimpleShardParser(new JSQLParser(), shardRouter));
// TODO: use 'ddrDataSource' to do your business here ...
```
