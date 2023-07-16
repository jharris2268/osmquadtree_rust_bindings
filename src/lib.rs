mod count;
mod elements;
mod readpbf;
mod messaging;
mod sortblocks;
use pyo3::prelude::*;

mod geometry;








#[pymodule]
fn rust(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    count::wrap_count(m)?;
    elements::wrap_elements(m)?;
    readpbf::wrap_readpbf(m)?;
    messaging::wrap_messaging(m)?;
    sortblocks::wrap_sortblocks(m)?;
    geometry::wrap_geometry(m)?;
    Ok(())
}
