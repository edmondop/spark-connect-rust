use crate::error::DeserializationError;
use crate::error::SparkError;
use crate::spark::execute_plan_response::ArrowBatch;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use std::io::Read;

pub struct ArrowBatchReader {
    batch: ArrowBatch,
}

impl Read for ArrowBatchReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.batch.data.as_slice(), buf)
    }
}

pub fn deserialize(batch: ArrowBatch) -> Result<Vec<RecordBatch>, SparkError> {
    let wrapper = ArrowBatchReader { batch };
    let reader = StreamReader::try_new(wrapper, None)?;
    let mut rows = Vec::new();
    for record in reader {
        rows.push(record?);
    }
    Ok(rows)
}

impl From<ArrowError> for SparkError {
    fn from(err: ArrowError) -> Self {
        SparkError::DeserializationFailed(DeserializationError(err.to_string()))
    }
}
