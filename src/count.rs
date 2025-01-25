use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::exceptions::*;
use pyo3::types::{PyList,PyDict,PyModuleMethods};
use std::sync::{Arc,RwLock};
use osmquadtree::count::{CountBlocks};
use osmquadtree::elements::Changetype;

use crate::ErrorWrapped;

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

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.node))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("NodeCount [{}]", self.inner.get()?.node.num))
    }
}




#[pyclass]
pub struct NodeCountChange {
    inner: CountChange,
    //key: String
    changetype: Changetype
}

       
    

#[pymethods]
impl NodeCountChange {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.node[&self.changetype].num) }
    
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.node[&self.changetype].min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.node[&self.changetype].max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.node[&self.changetype].min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.node[&self.changetype].max_ts) }
    
    #[getter]
    pub fn min_lon(&self) -> PyResult<i32> { Ok(self.inner.get()?.node[&self.changetype].min_lon) }
    
    #[getter]
    pub fn min_lat(&self) -> PyResult<i32> { Ok(self.inner.get()?.node[&self.changetype].min_lat) }
    
    #[getter]
    pub fn max_lon(&self) -> PyResult<i32> { Ok(self.inner.get()?.node[&self.changetype].max_lon) }
    
    #[getter]
    pub fn max_lat(&self) -> PyResult<i32> { Ok(self.inner.get()?.node[&self.changetype].max_lat) }

    fn __str__(&self) -> PyResult<String> {
        let x= &self.inner.get()?.node[&self.changetype];
        Ok(format!("{}", x))
        
        /*let res = Vec::new();
        for (key,cc) in &self.inner.get()?.node {
            res.push(format!("  {}: {}", key, cc));
        }
        Ok(format!("nodes:\n{}", res.join("\n")))*/
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("NodeCountChange [{}]", self.inner.get()?.node[&self.changetype].num))
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
    

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.way))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("WayCount [{}]", self.inner.get()?.way.num))
    }
}




#[pyclass]
pub struct WayCountChange {
    inner: CountChange,
    //key: String
    changetype: Changetype
}

#[pymethods]
impl WayCountChange {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].num) }
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].max_ts) }
    
    #[getter]
    pub fn num_refs(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].num_refs) }
    
    #[getter]
    pub fn max_refs_len(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].max_refs_len) }
    
    #[getter]
    pub fn min_ref(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].min_ref) }
    
    #[getter]
    pub fn max_ref(&self) -> PyResult<i64> { Ok(self.inner.get()?.way[&self.changetype].max_ref) }
    

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.way[&self.changetype]))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("WayCount [{}]", self.inner.get()?.way[&self.changetype].num))
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
    

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.relation))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("RelationCount [{}]", self.inner.get()?.relation.num))
    }
}

#[pyclass]
pub struct RelationCountChange {
    inner: CountChange,
    //key: String
    changetype: Changetype
}
#[pymethods]
impl RelationCountChange {
    #[getter]
    pub fn num(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].num) }
    
    #[getter]
    pub fn min_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].min_id) }
    
    #[getter]
    pub fn max_id(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].max_id) }
    
    #[getter]
    pub fn min_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].min_ts) }
    
    #[getter]
    pub fn max_ts(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].max_ts) }
    
    #[getter]
    pub fn num_mems(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].num_mems) }
    
    #[getter]
    pub fn max_mems_len(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].max_mems_len) }
    
    #[getter]
    pub fn num_empties(&self) -> PyResult<i64> { Ok(self.inner.get()?.relation[&self.changetype].num_empties) }
    

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner.get()?.relation[&self.changetype]))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("RelationCount [{}]", self.inner.get()?.relation[&self.changetype].num))
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
        Ok(NodeCount{inner: Count{inner: self.inner.clone()}})
    }
        
        
    
    #[getter]
    pub fn way(&self) -> PyResult<WayCount> {
        Ok(WayCount{inner: Count{inner: self.inner.clone()}})
    }
    
    #[getter]
    pub fn relation(&self) -> PyResult<RelationCount> {
        Ok(RelationCount{inner: Count{inner: self.inner.clone()}})
    }
    
    #[getter]
    pub fn num_blocks(&self) -> PyResult<i64> {
        Ok(self.get()?.num_blocks)
    }
    
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.get()?))
    }
    fn __repr__(&self) -> PyResult<String> {
        let c = self.get()?;
        Ok(format!("Count [{} blocks, {}, {}, {}]", c.num_blocks, c.node.num, c.way.num, c.relation.num))
    }
}







#[pyclass]
pub struct CountChange {
    inner: Arc<RwLock<osmquadtree::count::CountChange>>
}
impl CountChange {
    fn get<'a>(&'a self) -> PyResult<std::sync::RwLockReadGuard<osmquadtree::count::CountChange>> {
        
        match self.inner.read() {
            Ok(r) => Ok(r),
            Err(e) => Err(PyValueError::new_err(format!("{}", e)))
        }
       
    }
    fn get_write<'a>(&'a self) -> PyResult<std::sync::RwLockWriteGuard<osmquadtree::count::CountChange>> {
        
        
        match self.inner.write() {
            Ok(r) => Ok(r),
            Err(e) => Err(PyValueError::new_err(format!("{}", e)))
        }
        
    }
}
#[pymethods]
impl CountChange {
    
    #[new]
    pub fn new() -> PyResult<CountChange> {
        Ok(CountChange{ inner: Arc::new(RwLock::new(osmquadtree::count::CountChange::new())) })
    }
    
    pub fn add_primitive(&mut self, pb: &crate::elements::PrimitiveBlock) -> PyResult<()> {
        self.get_write()?.add_primitive(pb.get_inner());
        Ok(())
    }
    
        
    
    #[getter]
    pub fn node(&self, py: Python) -> PyResult<PyObject> {
        let res = PyDict::new(py);
        for (key,_cc) in &self.get()?.node {
            res.set_item(
                format!("{}",key), 
                NodeCountChange{
                        inner: CountChange{inner: self.inner.clone()},
                        changetype: *key
                });
        }
        Ok(res.into())
    }
    
    #[getter]
    pub fn num_blocks(&self) -> PyResult<i64> {
        Ok(self.get()?.num_blocks)
    }
    
        
    #[getter]
    pub fn way(&self, py: Python) -> PyResult<PyObject> {
        let res = PyDict::new(py);
        for (key,_cc) in &self.get()?.way {
            res.set_item(
                format!("{}",key), 
                WayCountChange{
                        inner: CountChange{inner: self.inner.clone()},
                        changetype: *key
                });
        }
        Ok(res.into())
    }
    
    #[getter]
    pub fn relation(&self, py: Python) -> PyResult<PyObject> {
        let res = PyDict::new(py);
        for (key,_cc) in &self.get()?.relation {
            res.set_item(
                format!("{}",key), 
                RelationCountChange{
                        inner: CountChange{inner: self.inner.clone()},
                        changetype: *key
                });
        }
        Ok(res.into())
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.get()?))
    }
    fn __repr__(&self) -> PyResult<String> {
        let c = self.get()?;
        
        let mut nn=0; let mut ww=0; let mut rr=0;
        for (_,cc) in &c.node {
            nn+=cc.num;
        }
        for (_,cc) in &c.way {
            ww+=cc.num;
        }
        for (_,cc) in &c.relation {
            rr+=cc.num;
        }
        
        Ok(format!("CountChange [{} blocks, {}, {}, {}]", c.num_blocks, nn,ww,rr))
    }
}















/// Parses the File from the specified Path into a document
#[pyfunction]
#[pyo3(signature = (fname, use_primitive=false, numchan=4, filter_in=None, tstamp=None))]
fn call_count(py: Python,
    fname: &str,
    use_primitive: bool,
    numchan: usize,
    filter_in: Option<&str>,
    tstamp: Option<&str>) -> PyResult<PyObject> {
    
    
    let op = || osmquadtree::count::call_count(fname, use_primitive, numchan, filter_in, tstamp);
    
    let res = 
        if numchan == 0 {
            op()
        
        } else {
            py.allow_threads(op)
        };
    
    
    match res {
        Err(e) => Err(PyErr::from(ErrorWrapped{e:e})),
        Ok(osmquadtree::count::CountAny::Count(cc)) => Ok(Count{inner: Arc::new(RwLock::new(cc)) }.into_py(py)),
        Ok(osmquadtree::count::CountAny::CountChange(cc)) => Ok(CountChange{inner: Arc::new(RwLock::new(cc)) }.into_py(py)),
    }
        
    
}



use crate::elements::prep_element_tuple;

#[pyfunction]
fn compare_pbf_files(py: Python, left: &str, right: &str, numchan: usize, max_result_len: usize) -> PyResult<PyObject> {
    
    let (eles,users,count) = match py.allow_threads( || {
    
        let left_iter = osmquadtree::pbfformat::iter_elements_flat(left, numchan/2)?;
        let right_iter = osmquadtree::pbfformat::iter_elements_flat(right, numchan/2)?;
        osmquadtree::elements::compare_element_iters(left_iter,right_iter,max_result_len)
    }) {
        Ok(x) => Ok(x), 
        Err(e) => Err(ErrorWrapped{e:e})
    }?;
    
    let list = PyList::empty(py);
    for ele in eles {
        match &ele {
            osmquadtree::elements::ElementCompare::OnlyLeft(l) => { list.append::<PyObject>(("OnlyLeft", prep_element_tuple(py,l)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::OnlyRight(r) => { list.append::<PyObject>(("OnlyRight", prep_element_tuple(py,r)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::DifferentInfo(l,r) => { list.append::<PyObject>(("DifferentInfo", prep_element_tuple(py,l)?,prep_element_tuple(py,r)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::DifferentTags(l,r) => { list.append::<PyObject>(("DifferentTags", prep_element_tuple(py,l)?,prep_element_tuple(py,r)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::DifferentData(l,r) => { list.append::<PyObject>(("DifferentData", prep_element_tuple(py,l)?,prep_element_tuple(py,r)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::DifferentQuadtree(l,r) => { list.append::<PyObject>(("DifferentQuadtree", prep_element_tuple(py,l)?,prep_element_tuple(py,r)?).into_py(py))?; },
            osmquadtree::elements::ElementCompare::Same => {},
            osmquadtree::elements::ElementCompare::ChangedUserName(ln,rn) => { list.append::<PyObject>(("ChangedUserName",ln,rn).into_py(py))?;},
        }
    }
    
    Ok((list, users,count).into_py(py))
}

#[pyfunction]
fn compare_pbf_files_json(py: Python, left: &str, right: &str, numchan: usize, outfn: &str) -> PyResult<PyObject> {
    
    let (users,count) = match py.allow_threads( || {
    
        let left_iter = osmquadtree::pbfformat::iter_elements_flat(left, numchan/2)?;
        let right_iter = osmquadtree::pbfformat::iter_elements_flat(right, numchan/2)?;
        osmquadtree::elements::compare_element_iters_json(left_iter,right_iter,outfn)
    }) {
        Ok(x) => Ok(x), 
        Err(e) => Err(ErrorWrapped{e:e})
    }?;
    
   
    
    Ok((users,count).into_py(py))
}

#[pyfunction]
fn timestamp_string(py: Python, ts: i64) -> PyResult<PyObject> {
    Ok(osmquadtree::utils::timestamp_string(ts).into_py(py))
}

pub(crate) fn wrap_count(m: &Bound<'_, PyModule>) -> PyResult<()> {
    
    m.add_wrapped(wrap_pyfunction!(call_count))?;
    m.add_class::<NodeCount>()?;
    m.add_class::<WayCount>()?;
    m.add_class::<RelationCount>()?;
    m.add_class::<Count>()?;
    
    m.add_class::<NodeCountChange>()?;
    m.add_class::<WayCountChange>()?;
    m.add_class::<RelationCountChange>()?;
    m.add_class::<CountChange>()?;
    
    m.add_wrapped(wrap_pyfunction!(compare_pbf_files))?;
    m.add_wrapped(wrap_pyfunction!(compare_pbf_files_json))?;
    
    m.add_wrapped(wrap_pyfunction!(timestamp_string))?;
    
    Ok(())
}
