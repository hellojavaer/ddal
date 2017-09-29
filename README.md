# DDAL

[![Build Status](https://travis-ci.org/hellojavaer/ddal.svg?branch=master)](https://travis-ci.org/hellojavaer/ddal)
[![GitHub release](https://img.shields.io/github/release/hellojavaer/ddal.svg)](https://github.com/hellojavaer/ddal/releases)

DDAL(Distributed Data Access Layer) is a simple solution to access database shard.
<img src="https://github.com/hellojavaer/ddal/blob/master/doc/img/design_01.jpeg" width = "590" height = "390" alt="design" align=center />

## License

DDAL is dual licensed under **LGPL V2.1** and **Apache Software License, Version 2.0**.

## Qick start

See [ddal-example](https://github.com/hellojavaer/ddal/tree/master/ddal-example)

## Documentation

- [Documentation Home](https://github.com/hellojavaer/ddal/wiki)
- [Frequently Asked Questions](https://github.com/hellojavaer/ddal/wiki/faq)


## Download

http://repo1.maven.org/maven2/org/hellojavaer/ddal/

## Maven

```xml
<dependency>
    <groupId>org.hellojavaer.ddal</groupId>
    <artifactId>ddal-datasource</artifactId>
    <version>x.x.x</version>
</dependency>
```

### [Release Notes](https://github.com/hellojavaer/ddal/releases)

## Extensions in the latest version 1.0.0.M6

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
