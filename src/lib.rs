mod count;

use pyo3::prelude::*;

#[pymodule]
fn rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    count::wrap_count(m)?;
    
    Ok(())
}
