# DataFusion 2021 Roadmap

## Summary

This RFC aims to build consensus on a roadmap for DataFusion for 2021. There is a separate RFC for the core Arrow crate (see [ARROW-10972](https://issues.apache.org/jira/browse/ARROW-10972)).

## Current Status

With the release of Apache Arrow 3.0.0 in January 2021, DataFusion will have matured significantly compared to the 2.0.0 release. Some of the highlights of this release are:

- Move to stable Rust (except when SIMD support is enabled)
- Support for equi-joins
- Support for more TPC-H benchmark queries
- Preliminary support for query optimizations based on statistics
- Extensive performance and parallelism improvements
- Support for additional operators and expressions, largely driven by the TPC-H benchmarks

With this release, DataFusion is capable of supporting a wider range of real-world queries with reasonable performance.

# Goals for 2021

We expect that there will be three more releases of DataFusion in 2021 based on the roughly quarterly release cadence of Apache Arrow, and the remainder of this document gives an overview of various areas of focus for these releases.

## Benchmarks

The TPC-H benchmarks are proving to be a good way to test DataFusion's functionality and performance compared to other query engines. It would be good to add the required functionality to be able to support running all 22 queries.

It would also be good to have the benchmarks automated to run against various size data sets (perhaps scale factors 1, 10, and 100) in a way that is accessible to all contributors. Ideally, we should set this up in CI.

## Scalability

There are a number of ways in which we can improve scalability.

### Introduce a Scheduler

The threading and execution model in DataFusion is still quite simplistic and although it generally works well for single-table and single-join queries, it does not scale well for more complex queries that have many nested joins.

One issue is that we try and execute the whole query graph concurrently, which can be inefficient. By implementing a scheduler, we can break physical query plans down into stages and tasks and manage the concurrency of tasks being executed to make better use of available cores.

This approach could also potentially have benefits by making accurate statistics available as each query stage completes, allowing the remainder of the plan to be optimized based on those statistics.

### Finer-grained Partitioning of Parquet Reads

The unit of parallelism when reading Parquet files is individual files. This means that queries do not scale well when there are fewer Parquet files than available cores. This could be improved by partitioning based on Parquet row groups.

### Implement Sort-merge Join

The current hash join works well when one side of the join can be loaded into memory but cannot scale beyond the available RAM.

The advantage of implementing Sort-Merge Join is that we can  sort the left and right partitions, and write the intermediate results  to disk, and then stream both sides of the join by merging these sorted partitions and we do not need to load one side into memory. At most, we need to load all batches from both sides that contain the next set of join key values.

We would still want to default to hash join when we know that the  build-side can fit into memory since it is more efficient than using a sort-merge join.

## Flexible Configuration Settings

DataFusion has very few configuration settings currently and each time we add a new configuration settings, it typically takes some effort to plumb these through. If we were to add a generic way to provide configuration settings as key-value pairs, it would be easy for new operators to take advantage of new configuration settings to allow more fine-grained query performance tuning.

## Continue with Cost-based Optimizations

DataFusion 3.0.0 introduced some basic cost-based optimizations based on statistics, such as choosing the build-side of a hash join based on the number of input rows, when known. There are opportunities to extend this work.

## Support Additional Operators and Expressions

There will be ongoing work to implement additional operators and expressions to better support real-world workloads.
