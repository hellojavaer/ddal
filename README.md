# DDAL

[![Build Status](https://travis-ci.org/hellojavaer/ddal.svg?branch=master)](https://travis-ci.org/hellojavaer/ddal)
[![GitHub release](https://img.shields.io/github/release/hellojavaer/ddal.svg)](https://github.com/hellojavaer/ddal/releases)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.hellojavaer.ddal/ddal-datasource/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.hellojavaer.ddal/ddal-datasource/)

DDAL(Distributed Data Access Layer) is a simple solution to access database shard.

<img src="https://github.com/hellojavaer/ddal/blob/master/doc/img/design_02.jpeg" width = "590" height = "390" alt="design" align=center />

## License

DDAL is dual licensed under **LGPL V2.1** and **Apache Software License, Version 2.0**.


## Quick start

- add the following dependency in your pom.xml

```xml
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-datasource</artifactId>
    <version>1.1.1-RELEASE</version>
</dependency>
```

- use DefaultDDALDataSource to proxy the orginal dataSource

```
<bean name="dataSource" class="org.hellojavaer.ddal.datasource.DefaultDDALDataSource">
    <constructor-arg index="0" value="jdbc:ddal:thick:classpath:/datasource.xml"/>
    <!-- <constructor-arg index="0" value="jdbc:ddal:thick:http://{host}:{port}/{appName}"/> -->
</bean>
```

- see [datasource.xml](https://github.com/hellojavaer/ddal/blob/master/ddal-example/ddal-example-example0/src/main/resources/datasource.xml)

- see [a full example](https://github.com/hellojavaer/ddal/tree/master/ddal-example)

## Features

- Only need to proxy the original datasources for business
- Fully support ACID
- Support insert, select, update and delete expression
- Support join(inner join,left join,right join,full jion), sub-select, union all and exists expression
- Support table alias
- Support no shard-key schema/table routing
- Support '=', in' and 'between' operation to route schema/table and support mixed sql parameter and jdbc parameter in routing values
    - eg: 'select * from tb where id in(1,?,3)'
    - eg: 'select * from tb where id between 1 and ?'
- Support Date, Timestamp, Byte, Short, Integer, Long, String, Character, Hex value type for shard-value
- Support annotation routing
- Support custom route rule (eg: '{scName}_{format('%02d', sdValue % 4)}')
- Support route rule binding on a schema (instead of a table)
- Support scan all schemas and tables
- Support limit check
- Support read-write splitting
- Support load balance of read
- Support config at server-side
- Support db cluster route
- Provide multiple sequence implements

## Download

http://repo1.maven.org/maven2/org/hellojavaer/ddal/

## Documentation

- [Documentation Home](https://github.com/hellojavaer/ddal/wiki)
- [Frequently Asked Questions](https://github.com/hellojavaer/ddal/wiki/faq)


## [Release Notes](https://github.com/hellojavaer/ddal/releases)

## Extensions in the latest version 1.1.0-RELEASE

- support DB cluster route
- enhance ShardRouteUtils

## Extensions in version 1.0.1-RELEASE

- Support route rule binding on a schema (instead of a table)

```
<bean class="org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding">
    <property name="scName" value="base"/>
    <property name="rule">
        <bean class="org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule">
            <property name="scRouteRule" value="{scName}_{format('%02d', sdValue % 2)}"/>
            <property name="tbRouteRule" value="{tbName}_{format('%04d', sdValue % 8)}"/>
        </bean>
    </property>
    <property name="sdKey" value="id"/>
    <property name="sdValues" value="[0..7]"/>
</bean>
```

## Extensions in version 1.0.0-RELEASE

- upgrade jsqlparser's version to 1.2 to support more sql features
- optimize JSQLParserAdapter

## Extensions in version 1.0.0.M7

- support custom protocol jdbc:ddal:

## Extensions in version 1.0.0.M6

- implement DefaultDDALDataSource in ddal-datasource module

```
<bean name="dataSource" class="org.hellojavaer.ddal.datasource.DefaultDDALDataSource">
    <constructor-arg index="0" value="jdbc:ddal:thick:classpath:/datasource.xml"/>
</bean>
```

- implement DivideShardRouteRule for range route

## Extensions in version 1.0.0.M5

- optimize route rule expression parser

```
// older
SpelShardRouteRule rule = new SpelShardRouteRule();
rule.setScRouteRule("{#scName}_{#format('%02d', #sdValue % 4)}");
rule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue % 8)}");

// newer
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
