mod count;
mod elements;
mod readpbf;
mod messaging;
mod sortblocks;
use pyo3::prelude::*;

mod geometry;

//use osmquadtree::Error;
use pyo3::exceptions::PyOSError;

#[derive(Debug)]
struct ErrorWrapped {
    e: osmquadtree::Error
}

impl std::fmt::Display for ErrorWrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.e)
    }
}

impl std::error::Error for ErrorWrapped {}

impl std::convert::From<osmquadtree::Error> for ErrorWrapped {
    fn from(err: osmquadtree::Error) -> ErrorWrapped {
        ErrorWrapped{e: err}
    }
}


impl std::convert::From<ErrorWrapped> for pyo3::PyErr {
    fn from(err: ErrorWrapped) -> pyo3::PyErr {
        PyOSError::new_err(err.e.to_string())
    }
}



#[pymodule]
fn rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    count::wrap_count(m)?;
    elements::wrap_elements(m)?;
    readpbf::wrap_readpbf(m)?;
    messaging::wrap_messaging(m)?;
    sortblocks::wrap_sortblocks(m)?;
    geometry::wrap_geometry(m)?;
    Ok(())
}
