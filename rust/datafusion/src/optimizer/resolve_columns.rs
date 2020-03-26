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

//! Optimizer rule to replace UnresolvedColumns with Columns

use crate::error::Result;
use crate::logicalplan::LogicalPlan;
use crate::logicalplan::{Expr, LogicalPlanBuilder};
use crate::optimizer::optimizer::OptimizerRule;
use arrow::datatypes::Schema;
use std::sync::Arc;

/// Replace UnresolvedColumns with Columns
pub struct ResolveColumnsRule {}

impl ResolveColumnsRule {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ResolveColumnsRule {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<Arc<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection { input, expr, .. } => Ok(Arc::new(
                LogicalPlanBuilder::from(input)
                    .project(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?,
            )),
            LogicalPlan::Selection { expr, input } => Ok(Arc::new(
                LogicalPlanBuilder::from(input)
                    .filter(rewrite_expr(expr, &input.schema())?)?
                    .build()?,
            )),
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => Ok(Arc::new(
                LogicalPlanBuilder::from(input)
                    .aggregate(
                        rewrite_expr_list(group_expr, &input.schema())?,
                        rewrite_expr_list(aggr_expr, &input.schema())?,
                    )?
                    .build()?,
            )),
            LogicalPlan::Sort { input, expr, .. } => Ok(Arc::new(
                LogicalPlanBuilder::from(input)
                    .sort(rewrite_expr_list(expr, &input.schema())?)?
                    .build()?,
            )),
            _ => Ok(Arc::new(plan.clone())),
        }
    }
}
fn rewrite_expr_list(expr: &Vec<Expr>, schema: &Schema) -> Result<Vec<Expr>> {
    Ok(expr
        .iter()
        .map(|e| rewrite_expr(e, schema))
        .collect::<Result<Vec<_>>>()?)
}

fn rewrite_expr(expr: &Expr, schema: &Schema) -> Result<Expr> {
    match expr {
        Expr::UnresolvedColumn(name) => Ok(Expr::Column(schema.index_of(&name)?)),
        Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
            left: Arc::new(rewrite_expr(&left, schema)?),
            op: op.clone(),
            right: Arc::new(rewrite_expr(&right, schema)?),
        }),
        //TODO recurse into other expr
        _ => Ok(expr.clone()),
    }
}
