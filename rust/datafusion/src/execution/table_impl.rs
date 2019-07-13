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

//! Implementation of Table API

use std::sync::Arc;

use crate::arrow::datatypes::{Field, Schema};
use crate::error::{ExecutionError, Result};
use crate::logicalplan::Expr::Literal;
use crate::logicalplan::ScalarValue;
use crate::logicalplan::{Expr, LogicalPlan};
use crate::table::*;

/// Implementation of Table API
pub struct TableImpl {
    plan: Arc<LogicalPlan>,
}

impl TableImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

impl Table for TableImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<Table>> {
        let schema = self.plan.schema();
        let mut projection_index: Vec<usize> = Vec::with_capacity(columns.len());
        let mut expr: Vec<Expr> = Vec::with_capacity(columns.len());

        for column_name in columns {
            let i = self.column_index(column_name)?;
            projection_index.push(i);
            expr.push(Expr::Column(i));
        }

        Ok(Arc::new(TableImpl::new(Arc::new(
            LogicalPlan::Projection {
                expr,
                input: self.plan.clone(),
                schema: projection(&schema, &projection_index)?,
            },
        ))))
    }

    /// Limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<Table>> {
        Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::Limit {
            expr: Literal(ScalarValue::UInt32(n as u32)),
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        }))))
    }

    /// Convert to logical plan
    fn to_logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }

    /// Return an expression representing a column within this table
    fn col(&self, name: &str) -> Result<Expr> {
        Ok(Expr::Column(self.column_index(name)?))
    }

    /// Return the index of a column within this table's schema
    fn column_index(&self, name: &str) -> Result<usize> {
        let schema = self.plan.schema();
        match schema.column_with_name(name) {
            Some((i, _)) => {
                Ok(i)
            }
            _ => {
                Err(ExecutionError::InvalidColumn(format!(
                    "No column named '{}'",
                    name
                )))
            }
        }
    }
}



/// Create a new schema by applying a projection to this schema's fields
fn projection(schema: &Schema, projection: &Vec<usize>) -> Result<Arc<Schema>> {
    let mut fields: Vec<Field> = Vec::with_capacity(projection.len());
    for i in projection {
        if *i < schema.fields().len() {
            fields.push(schema.field(*i).clone());
        } else {
            return Err(ExecutionError::InvalidColumn(format!(
                "Invalid column index {} in projection",
                i
            )));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use arrow::datatypes::*;
    use std::env;

    #[test]
    fn column_index() {
        let t = test_table();
        assert_eq!(0, t.column_index("c1").unwrap());
        assert_eq!(1, t.column_index("c2").unwrap());
        assert_eq!(12, t.column_index("c13").unwrap());
    }

    #[test]
    fn select_columns() {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx);

        let t = ctx.table("aggregate_test_100").unwrap();

        let example = t
            .select_columns(vec!["c1", "c2", "c11"])
            .unwrap()
            .limit(10)
            .unwrap();

        let plan = example.to_logical_plan();

        assert_eq!("Limit: UInt32(10)\n  Projection: #0, #1, #10\n    TableScan: aggregate_test_100 projection=None", format!("{:?}", plan));
    }

    fn test_table() -> Arc<dyn Table + 'static> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx);
        ctx.table("aggregate_test_100").unwrap()
    }

    fn register_aggregate_csv(ctx: &mut ExecutionContext) {
        let schema = aggr_test_schema();
        let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            &schema,
            true,
        );
    }

    fn aggr_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c4", DataType::Int16, false),
            Field::new("c5", DataType::Int32, false),
            Field::new("c6", DataType::Int64, false),
            Field::new("c7", DataType::UInt8, false),
            Field::new("c8", DataType::UInt16, false),
            Field::new("c9", DataType::UInt32, false),
            Field::new("c10", DataType::UInt64, false),
            Field::new("c11", DataType::Float32, false),
            Field::new("c12", DataType::Float64, false),
            Field::new("c13", DataType::Utf8, false),
        ]))
    }

}
