use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::exceptions::{PyIndexError,PyValueError};
use crate::elements::Quadtree;
use std::sync::Arc;

#[pyfunction]
#[pyo3(signature = (fname, outfn=None,qt_level=17,qt_buffer=0.05, mode=None,keep_temps=false, numchan=4, ram_gb=8))]
pub fn run_calcqts(py: Python,
    fname: &str, 
    outfn: Option<&str>, 
    qt_level: usize, 
    qt_buffer: f64, 
    mode: Option<&str>, 
    keep_temps: bool,
    numchan: usize,
    ram_gb: usize) -> PyResult<PyObject> {
    
    let (outfnf,lt,max_timestamp) = py.allow_threads( || osmquadtree::calcqts::run_calcqts(fname, outfn, qt_level, qt_buffer, mode, keep_temps, numchan,ram_gb))?;
    Ok((outfnf,lt.msgs.into_py(py),max_timestamp).into_py(py))
}

fn check_tree_idx(i: u32) -> Option<u32> {
    if i == 4294967295 {
        None
    } else {
        Some(i)
    }
}

fn quadtreetreeitem_tuple(py: Python, ii: &osmquadtree::sortblocks::QuadtreeTreeItem) -> PyResult<PyObject> {
    Ok((
            Quadtree::new(ii.qt), check_tree_idx(ii.parent),
            ii.weight, ii.total, 
            (check_tree_idx(ii.children[0]), check_tree_idx(ii.children[1]),
                check_tree_idx(ii.children[2]), check_tree_idx(ii.children[3]))).into_py(py))
}
#[pyclass]
pub struct QuadtreeTree {
    pub inner: Option<Box<osmquadtree::sortblocks::QuadtreeTree>>
}

impl QuadtreeTree {
    
    fn get_inner<'a>(&'a self) -> PyResult<&'a Box<osmquadtree::sortblocks::QuadtreeTree>> {
        match &self.inner {
            Some(t) => Ok(t),
            None => Err(PyValueError::new_err("null QuadtreeTree"))
        }
    }
    
    fn get_inner_mut<'a>(&'a mut self) -> PyResult<&'a mut Box<osmquadtree::sortblocks::QuadtreeTree>> {
        match &mut self.inner {
            Some(t) => Ok(t),
            None => Err(PyValueError::new_err("null QuadtreeTree"))
        }
    }
}

#[pymethods]
impl QuadtreeTree {
    #[new]
    pub fn new() -> PyResult<QuadtreeTree> {
        
        Ok(QuadtreeTree{inner: Some(Box::new(osmquadtree::sortblocks::QuadtreeTree::new()))})
    }
    
    pub fn num_entries(&self) -> PyResult<usize> {
        Ok(self.get_inner()?.num_entries())
    }
        
        
    pub fn total_weight(&self) -> PyResult<i64> {
        Ok(self.get_inner()?.total_weight())
    }
    
        
    pub fn find(&self, py: Python, q: Quadtree) -> PyResult<(usize,PyObject)> {
        let (p,ii) = self.get_inner()?.find(&q.inner);
        Ok((p,quadtreetreeitem_tuple(py,ii)?))
    }
    pub fn next(&self, i: u32) -> PyResult<Option<u32>> {
        Ok(check_tree_idx(self.get_inner()?.next(i)))
    }
    pub fn next_sibling(&self, i: u32) -> PyResult<Option<u32>> {
        Ok(check_tree_idx(self.get_inner()?.next_sibling(i)))
    }
    
    pub fn remove(&mut self, qt: Quadtree) -> PyResult<i64> {
        Ok(self.get_inner_mut()?.remove(&qt.inner))
    }
    
    pub fn add(&mut self, py: Python, qt: Quadtree, w: u32) -> PyResult<PyObject> {
        let ii = self.get_inner_mut()?.add(&qt.inner, w);
        quadtreetreeitem_tuple(py, ii)
    }
    

/*    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }*/
    fn __repr__(&self) -> PyResult<String> {
        match &self.inner {
            Some(t) => { Ok(format!("QuadtreeTree with {} entries", t.len())) },
            None => { Ok(format!("QuadtreeTree Null")) }
        }
            
    }
/*}



#[pyproto]
impl PySequenceProtocol for QuadtreeTree {*/
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.get_inner()?.len())
    }
    
    fn __getitem__(&self, idx: isize) -> PyResult<PyObject> {
        let t = self.get_inner()?;
        if idx < 0 || idx >= (t.len() as isize) {
            return Err(PyIndexError::new_err(format!("?? {}", idx)));
        }
        
        let ii = t.at(idx as u32);
        
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        Python::with_gil(|py| { 
            quadtreetreeitem_tuple(py, ii)
        })
        
    }
             
}

#[pyfunction]
pub fn prepare_quadtree_tree(py: Python, qtsfn: &str, numchan: usize, maxdepth: usize) -> PyResult<QuadtreeTree> {
    let tree = py.allow_threads( || osmquadtree::sortblocks::prepare_quadtree_tree(qtsfn, numchan, maxdepth))?;
    Ok(QuadtreeTree{inner: Some(tree)})
}


#[pyfunction]
pub fn find_tree_groups(py: Python, tree_py: &mut QuadtreeTree, target: i64, min_target: i64) -> PyResult<QuadtreeTree> {
    
    let tree = tree_py.inner.take().unwrap();
    
    let res = py.allow_threads(move || osmquadtree::sortblocks::find_tree_groups(tree, target, min_target))?;
    
    Ok(QuadtreeTree{inner: Some(res)})
}


#[pyfunction]
pub fn sort_blocks(
    py: Python, infn: &str, qtsfn: &str, outfn: &str, 
    groups_obj: &mut QuadtreeTree, numchan: usize, splitat: i64,
    tempinmem: bool, limit: usize, timestamp: i64, keep_temps: bool, compression_type: (String,u32)) -> PyResult<PyObject> {
        
    let mut lt = osmquadtree::utils::LogTimes::new();
    let ct = crate::readpbf::compression_type_from_string((&compression_type.0, compression_type.1))?;
    let groups = Arc::from(groups_obj.inner.take().unwrap());
    
    py.allow_threads(|| osmquadtree::sortblocks::sort_blocks(infn, qtsfn, outfn, groups, numchan, splitat, tempinmem, limit, timestamp, keep_temps, ct, &mut lt))?;
    
    //Ok(format!("{}", lt))
    Ok(lt.msgs.into_py(py))
}


pub(crate) fn wrap_sortblocks(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(run_calcqts))?;
    m.add_class::<QuadtreeTree>()?;
    m.add_wrapped(wrap_pyfunction!(prepare_quadtree_tree))?;
    m.add_wrapped(wrap_pyfunction!(find_tree_groups))?;
    m.add_wrapped(wrap_pyfunction!(sort_blocks))?;
    Ok(())
}
