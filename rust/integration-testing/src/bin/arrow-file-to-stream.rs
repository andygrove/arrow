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

use std::env;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};

use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::StreamWriter;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    eprintln!("Rust: arrow-file-to-stream; args={:?}", args);

    let filename = &args[0];
    eprintln!("Reading from Arrow file {}", filename);

    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    let mut reader = FileReader::try_new(reader)?;
    let schema = reader.schema();

    let writer = BufWriter::new(io::stdout());
    let mut writer = StreamWriter::try_new(writer, &schema)?;

    while let Some(batch) = reader.next()? {
        writer.write(&batch)?;
    }
    writer.finish()?;

    Ok(())
}
