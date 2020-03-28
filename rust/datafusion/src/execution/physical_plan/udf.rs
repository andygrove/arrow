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

//! UDF support

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, DataType};

use crate::error::Result;
use crate::execution::physical_plan::PhysicalExpr;

use std::sync::Arc;

/// Scalar UDF
pub type ScalarFunction = fn(input: &Vec<ArrayRef>) -> Result<ArrayRef>;

pub struct ScalarFunctionFoo {
    meta: ScalarFunctionMeta,
    f: ScalarFunction
}

impl ScalarFunctionFoo {
    pub fn meta() -> &ScalarFunctionMeta;
}

/// Scalar UDF Expression
pub struct ScalarFunctionMeta {
    name: String,
    args: Vec<Field>,
    return_type: DataType,
}

/// Scalar UDF Expression
pub struct ScalarFunctionExpr {
    name: String,
    f: Box<ScalarFunction>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: String,
        f: Box<ScalarFunction>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Self {
        Self {
            name,
            f,
            args,
            return_type,
        }
    }
}
