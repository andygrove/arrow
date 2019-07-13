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

//! Table API for building a logical query plan. This is similar to the Table API in Ibis
//! and the DataFrame API in Apache Spark

use crate::error::Result;
use crate::logicalplan::{LogicalPlan, Expr};
use std::sync::Arc;

/// Table is an abstraction of a logical query plan
pub trait Table {
    /// Select columns by name
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<Table>>;

    /// limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<Table>>;

    /// Return the logical plan
    fn to_logical_plan(&self) -> Arc<LogicalPlan>;

    /// Return an expression representing a column within this table
    fn col(&self, name: &str) -> Result<Expr>;

    /// Return the index of a column within this table's schema
    fn column_index(&self, name: &str) -> Result<usize>;

}
