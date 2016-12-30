# Linea

[![Join the chat at https://gitter.im/srotya/linea](https://badges.gitter.im/srotya/linea.svg)](https://gitter.im/srotya/linea?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/srotya/linea.svg?branch=master)](https://travis-ci.org/srotya/linea)
[![codecov](https://codecov.io/gh/srotya/linea/branch/master/graph/badge.svg)](https://codecov.io/gh/srotya/linea)

Linea is Latin for limit, is a stream processing framework leveraged in Tau. It's based on concepts from Apache Storm however it's written from scratch without using any Apache Storm code/dependencies.

Linea also uses an Apache Storm like API to enable easy adoption and familiarity for developers who would like to leverage this framework.

## Why Linea?

Linea is intended to be light-weight (3MB shaded jar), think hybrid of Storm and Akka/Vert.x

Profiling and tuning is relatively straight forward with Linea due to it's light weight design enabling quicker development to production cycles in comparison.

#### Usage scenarios

You can embed this in your own microservice providing it streaming capabilities without the overhead of complex integrations with existing frameworks.

Or

Something like Kafka Streams without the need for Kafka.

Or

To build your own Streaming system using this as building block.

### Sample Code

This is how simple it is to build a topology with Linea.

```
Map<String, String> conf = new HashMap<>();
conf.put(TopologyBuilder.WORKER_COUNT, "1");
conf.put(TopologyBuilder.ACKER_PARALLELISM, "6");
TopologyBuilder builder = new TopologyBuilder(conf);
builder = builder.addSpout(new TestSpout(), 8).addBolt(new PrinterBolt(), 6).start();
Thread.sleep(50000);
System.exit(1);
```

### Origins

Linea was conceived out of a series of evolutions of Tau / Wraith engines for Event Correlation, since the problem domain poses some scaling and optimization challenges for guaranteed stream processing systems. These requirements demand for stream processing framework that is easy to tune, easy to deploy and doesn't have a lot of moving parts or overhead.

### Why bother?
- Storm offers the amazing XOR Ledger design pattern (credit: Nathan Marz)
- Disruptor offers state-of-art inter-thread messaging and parallelism framework
- Headless clustering design pattern is needed for truly container native deployment / dynamic scaling
- Tau needs best of all worlds to be scalable, reliable and highly performant
- Need the ability for both a push and a pull model for streaming

### What's different?
Even though the lineage of Linea's concepts are from existing frameworks, there are a few key differences:

#### 1. Deployments

Linea doesn't require a cluster, rather has the ability to auto-cluster as instances are launched, ideally it should be deployed on a docker cluster (Swarm / Kubernetes) or across VMs.

#### 2. Protocols

Linea will attempt to use UDP. Why? XOR ledger already provides an application level acking mechanism therefore TCP acking is redundant.

#### 3. Push and Pull

Linea allows Spouts to have implement both a Push and Pull model since each Spout is provided it's own asynchronous executor where user defined code can run.

#### 4. 1 worker / 1 container

Linea is fundamentally built with containerization in mind and it's recommended to follow a strict 1 worker per container model where a container can be a Physical Machine or VM or Linux Container.

#### 5. Not multi-tenant

Linea is a framework, it's not an end-product in itself i.e. you can't run a Linea cluster by itself. It's like dropwizard, spring framework, vert.x etc. therefore there is no multi-tenancy or resource sharing support. The deployment mechanism is responsible for resource multi-tenancy similar to microservices.

## Components

`Event`: Linea's version of a Storm Tuple

`Bolt`: Custom code to execute

`BoltExecutor`: Operates a collection of Bolt instances in parallel

`Router`: Responsible for Routing events to the correct bolt instance across workers

`Acker`: A special Bolt that uses GROUPBY Routing Type

`ROUTING_TYPE`: Linea's version of Storm Grouping

`Columbus`: A gossip based worker discovery service
