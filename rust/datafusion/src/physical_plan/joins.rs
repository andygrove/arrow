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

use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::error::{ExecutionError, Result};
use crate::physical_plan::hash_aggregate::GroupByScalar;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HashInnerJoin {
    build_side: Arc<dyn ExecutionPlan>,
    build_side_keys: Vec<Arc<dyn PhysicalExpr>>,
    probe_side: Arc<dyn ExecutionPlan>,
    probe_side_keys: Vec<Arc<dyn PhysicalExpr>>,
    concurrent_tasks: usize,
    build_side_state: Arc<Mutex<Option<Arc<BuildSideState>>>>,
}

#[async_trait]
impl ExecutionPlan for HashInnerJoin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.probe_side.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.build_side.clone(), self.probe_side.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        // we only want to execute the build side once
        {
            let mut build_side = self.build_side_state.lock().unwrap();
            if build_side.is_none() {
                let merge =
                    MergeExec::new(self.build_side.clone(), self.concurrent_tasks);
                let it = merge.execute(0).await?;
                let mut it = it.lock().unwrap();

                let mut build_side_batches = vec![];
                while let Some(batch) = it.next_batch()? {
                    build_side_batches.push(batch);
                }

                *build_side = Some(Arc::new(BuildSideState {
                    build_side_batches,
                    build_side_index: Default::default(),
                }));
            }
        }

        let x: Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> = {
            match self.build_side_state.lock().unwrap().as_ref() {
                Some(ty) => Ok(Arc::new(Mutex::new(ProbeSideReader {
                    build_side_state: ty.clone(),
                    probe_side_reader: self.probe_side.execute(partition).await?,
                    probe_side_keys: self.probe_side_keys.clone(),
                }))),
                None => Err(ExecutionError::General("".to_string())),
            }
        };

        x
    }
}

#[derive(Debug, Clone)]
struct BuildSideState {
    build_side_batches: Vec<RecordBatch>,
    build_side_index: HashMap<Vec<Option<GroupByScalar>>, Vec<(usize, usize)>>,
}

#[derive(Clone)]
struct ProbeSideReader {
    build_side_state: Arc<BuildSideState>,
    probe_side_reader: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    probe_side_keys: Vec<Arc<dyn PhysicalExpr>>,
}

impl RecordBatchReader for ProbeSideReader {
    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let batch = self.probe_side_reader.lock().unwrap().next_batch()?;
        match batch {
            Some(b) => {
                let probe_join_keys: Vec<ArrayRef> = self
                    .probe_side_keys
                    .iter()
                    .map(|k| k.evaluate(&b))
                    .collect::<Result<Vec<_>>>()
                    .map_err(ExecutionError::into_arrow_external_error)?;

                // TODO perform actual join here against build side hash table

                Ok(Some(b))
            }
            None => Ok(None),
        }
    }
}
