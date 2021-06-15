mod count;
mod elements;
mod readpbf;
use pyo3::prelude::*;

#[pymodule]
fn rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    count::wrap_count(m)?;
    elements::wrap_elements(m)?;
    readpbf::wrap_readpbf(m)?;
    Ok(())
}
