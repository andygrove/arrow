// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines the merge plan for executing partitions in parallel and then merging the results
//! into a single partition

use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common::RecordBatchIterator;
use crate::execution::physical_plan::Partition;
use crate::execution::physical_plan::{common, ExecutionPlan};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct MergeExec {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of concurrent threads
    max_concurrency: usize,
}

impl MergeExec {
    /// Create a new MergeExec
    pub fn new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn Partition>>,
        max_concurrency: usize,
    ) -> Self {
        MergeExec {
            schema,
            partitions,
            max_concurrency,
        }
    }
}

impl ExecutionPlan for MergeExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        Ok(vec![Arc::new(MergePartition {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            max_concurrency: self.max_concurrency,
        })])
    }
}

#[derive(Debug)]
struct MergePartition {
    /// Input schema
    schema: SchemaRef,
    /// Input partitions
    partitions: Vec<Arc<dyn Partition>>,
    /// Maximum number of concurrent threads
    max_concurrency: usize,
}

fn collect_from_thread(
    thread: JoinHandle<Result<Vec<RecordBatch>>>,
    combined_results: &mut Vec<Arc<RecordBatch>>,
) -> Result<()> {
    match thread.join() {
        Ok(join) => {
            join?
                .iter()
                .for_each(|batch| combined_results.push(Arc::new(batch.clone())));
            Ok(())
        }
        Err(e) => Err(ExecutionError::General(format!(
            "Error collecting batches from thread: {:?}",
            e
        ))),
    }
}

impl Partition for MergePartition {
    fn execute(&self) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        let mut combined_results: Vec<Arc<RecordBatch>> = vec![];
        let mut threads: Vec<JoinHandle<Result<Vec<RecordBatch>>>> = vec![];

        for partition in &self.partitions {
            // limit number of concurrent threads
            if threads.len() == self.max_concurrency {
                // block and wait for this thread to finish
                collect_from_thread(threads.remove(0), &mut combined_results)?;
            }

            // launch thread for this partition
            let partition = partition.clone();
            threads.push(thread::spawn(move || {
                let it = partition.execute()?;
                common::collect(it)
            }));
        }

        // combine the results from each thread
        for thread in threads {
            collect_from_thread(thread, &mut combined_results)?;
        }

        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema.clone(),
            combined_results,
        ))))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::common;
    use crate::execution::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::test;

    #[test]
    fn merge() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;

        // input should have 4 partitions
        let input = csv.partitions()?;
        assert_eq!(input.len(), num_partitions);

        let merge = MergeExec::new(schema.clone(), input, 2);

        // output of MergeExec should have a single partition
        let merged = merge.partitions()?;
        assert_eq!(merged.len(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merged[0].execute()?;
        let batches = common::collect(iter)?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }
}
