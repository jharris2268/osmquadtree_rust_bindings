use pyo3::prelude::*;
use pyo3::{wrap_pyfunction,PyObjectProtocol};
use pyo3::exceptions::*;
use std::sync::{Arc,RwLock};
use osmquadtree::count::CountBlocks;
#[pyclass]
pub struct NodeCount {
    inner: Count
    //changetype: osmquadtree::elements::Changetype
}

#[pymethods]
impl NodeCount {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.node.num) }
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.node.min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.node.max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.node.min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.node.max_ts) }
    
    #[getter]
    pub fn min_lon(&self) -> PyResult<i32> { Ok(self.inner.get()?.node.min_lon) }
    
    #[getter]
    pub fn min_lat(&self) -> PyResult<i32> { Ok(self.inner.get()?.node.min_lat) }
    
    #[getter]
    pub fn max_lon(&self) -> PyResult<i32> { Ok(self.inner.get()?.node.max_lon) }
    
    #[getter]
    pub fn max_lat(&self) -> PyResult<i32> { Ok(self.inner.get()?.node.max_lat) }
}

#[pyproto]
impl PyObjectProtocol for NodeCount {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.node))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("NodeCount [{}]", self.inner.get()?.node.num))
    }
}

#[pyclass]
pub struct WayCount {
    inner: Count
}


#[pymethods]
impl WayCount {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.num) }
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.max_ts) }
    
    #[getter]
    pub fn num_refs(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.num_refs) }
    
    #[getter]
    pub fn max_refs_len(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.max_refs_len) }
    
    #[getter]
    pub fn min_ref(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.min_ref) }
    
    #[getter]
    pub fn max_ref(&self) -> PyResult<i64> { Ok(self.inner.get()?.way.max_ref) }
    
}

#[pyproto]
impl PyObjectProtocol for WayCount {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.way))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("WayCount [{}]", self.inner.get()?.way.num))
    }
}

#[pyclass]
pub struct RelationCount {
    inner: Count
}

#[pymethods]
impl RelationCount {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.num) }
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.max_ts) }
    
    #[getter]
    pub fn num_mems(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.num_mems) }
    
    #[getter]
    pub fn max_mems_len(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.max_mems_len) }
    
    #[getter]
    pub fn num_empties(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation.num_empties) }
    
}

#[pyproto]
impl PyObjectProtocol for RelationCount {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.relation))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("RelationCount [{}]", self.inner.get()?.relation.num))
    }
}

#[pyclass]
pub struct Count {
    inner: Arc<RwLock<osmquadtree::count::Count>>
}
impl Count {
    fn get<'a>(&'a self) -> PyResult<std::sync::RwLockReadGuard<osmquadtree::count::Count>> {
        
        match self.inner.read() {
            Ok(r) => Ok(r),
            Err(e) => Err(PyValueError::new_err(format!("{}", e)))
        }
        //&self.inner.node.read()
        /*
        match &self.inner {
            osmquadtree::count::CountAny::Count(inner) => Some(&inner.node),
            osmquadtree::count::CountAny::CountChange(inner) => inner.node.get(self.ct),
        }*/
    }
    fn get_write<'a>(&'a self) -> PyResult<std::sync::RwLockWriteGuard<osmquadtree::count::Count>> {
        
        match self.inner.write() {
            Ok(r) => Ok(r),
            Err(e) => Err(PyValueError::new_err(format!("{}", e)))
        }
        
    }
}
#[pymethods]
impl Count {
    
    #[new]
    pub fn new() -> PyResult<Count> {
        Ok(Count{ inner: Arc::new(RwLock::new(osmquadtree::count::Count::new())) })
    }
    
    pub fn add_primitive(&mut self, pb: &crate::elements::PrimitiveBlock) -> PyResult<()> {
        self.get_write()?.add_primitive(pb.get_inner());
        Ok(())
    }
    
        
    
    #[getter]
    pub fn node(&self) -> PyResult<NodeCount> {
        Ok(NodeCount{inner: Count{inner: self.inner.clone()}})//, changetype: osmquadtree::elements::Changetype::Normal})
    }
        
        
    
    #[getter]
    pub fn way(&self) -> PyResult<WayCount> {
        Ok(WayCount{inner: Count{inner: self.inner.clone()}})
    }
    
    #[getter]
    pub fn relation(&self) -> PyResult<RelationCount> {
        Ok(RelationCount{inner: Count{inner: self.inner.clone()}})
    }
}

#[pyproto]
impl PyObjectProtocol for Count {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.get()?))
    }
    fn __repr__(&self) -> PyResult<String> {
        let c = self.get()?;
        Ok(format!("Count [{}, {}, {}]", c.node.num, c.way.num, c.relation.num))
    }
}
/*
#[pyclass]
pub struct CountChange {
    inner: Arc<osmquadtree::count::CountChange>
}

#[pymethods]
impl CountChange {
    
    
    
    #[getter]
    pub fn node(&self) -> PyResult<PyDict> {
        Ok(NodeCount{inner: self.inner.clone()})
    }
        
        
    
    #[getter]
    pub fn way(&self) -> PyResult<PyDict> {
        Ok(WayCount{inner: self.inner.clone()})
    }
    
    #[getter]
    pub fn relation(&self) -> PyResult<PyDict> {
        Ok(RelationCount{inner: self.inner.clone()})
    }
}

#[pyproto]
impl PyObjectProtocol for Count {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Count [{}, {}, {}]", self.inner.node.num, self.inner.way.num, self.inner.relation.num))
    }
}

*/
/// Parses the File from the specified Path into a document
#[pyfunction]
fn call_count(py: Python,
    fname: &str,
    use_primitive: bool,
    numchan: usize,
    filter_in: Option<&str>) -> PyResult<Count> {
    
    
    let op = || osmquadtree::count::call_count(fname, use_primitive, numchan, filter_in);
    
    let res = 
        if numchan == 0 {
            op()
        
        } else {
            py.allow_threads(op)
        };
    
    
    match res {
        Err(e) => Err(PyErr::from(e)),
        Ok(osmquadtree::count::CountAny::Count(cc)) => Ok(Count{inner: Arc::new(RwLock::new(cc)) }),
        Ok(osmquadtree::count::CountAny::CountChange(_)) => Err(PyNotImplementedError::new_err("not impl"))
    }
        
    
}

pub(crate) fn wrap_count(m: &PyModule) -> PyResult<()> {
    
    m.add_wrapped(wrap_pyfunction!(call_count))?;
    m.add_class::<NodeCount>()?;
    m.add_class::<WayCount>()?;
    m.add_class::<RelationCount>()?;
    m.add_class::<Count>()?;
    
    Ok(())
}
