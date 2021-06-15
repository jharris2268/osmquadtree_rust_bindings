use pyo3::prelude::*;
//use pyo3::{wrap_pyfunction,PyObjectProtocol};
use pyo3::types::PyList;
use pyo3::exceptions::*;
//use std::sync::Arc;
use std::io::{Seek,SeekFrom,BufReader};
use std::fs::File;

#[pyclass]
struct FileBlock {
    inner: osmquadtree::pbfformat::FileBlock,
}

#[pymethods]
impl FileBlock {
    
    #[getter]
    pub fn pos(&self) -> PyResult<u64> { Ok(self.inner.pos) }
    
    #[getter]
    pub fn len(&self) -> PyResult<u64> { Ok(self.inner.len) }
    
    #[getter]
    pub fn block_type(&self) -> PyResult<String> { Ok(self.inner.block_type.clone()) }
    
    #[getter]
    pub fn data_raw(&self) -> PyResult<Vec<u8>> { Ok(self.inner.data_raw.clone()) }
    
    #[getter]
    pub fn is_comp(&self) -> PyResult<bool> { Ok(self.inner.is_comp) }
    
    #[getter]
    pub fn data(&self) -> PyResult<Vec<u8>> { Ok(self.inner.data()) }
    
}

#[pyclass]
struct ReadPbfFile {
    fname: String,
    fbuf: BufReader<File>,
}

#[pymethods]
impl ReadPbfFile {
    #[new]
    fn new(fname: &str) -> PyResult<Self> {
        let fobj = File::open(fname)?;
        let fbuf = BufReader::new(fobj);        
        Ok(ReadPbfFile{fname: String::from(fname), fbuf: fbuf})
        
    }
    
    
    pub fn position(&mut self) -> PyResult<u64> {
        Ok(osmquadtree::pbfformat::file_position(&mut self.fbuf)?)
    }
    
    pub fn next(&mut self) -> PyResult<FileBlock> {
        let fb = osmquadtree::pbfformat::read_file_block(&mut self.fbuf)?;
        Ok(FileBlock{inner: fb})
    }
    
    pub fn read_at(&mut self, pos: u64) -> PyResult<FileBlock> {
        self.fbuf.seek(SeekFrom::Start(pos))?;
        self.next()
    }
    
    pub fn get_header(&mut self) -> PyResult<HeaderBlock> {
        self.fbuf.seek(SeekFrom::Start(0))?;
        let fb = osmquadtree::pbfformat::read_file_block(&mut self.fbuf)?;
        if fb.block_type == "OSMHeader" {
            let hb = osmquadtree::pbfformat::HeaderBlock::read(fb.pos, &fb.data(), &self.fname)?;
            Ok(HeaderBlock{inner: hb})
        } else {
            self.fbuf.seek(SeekFrom::Start(0))?;
            Err(PyValueError::new_err("first block not an OSMHeader"))
        }
        
    }
    
    
    
    pub fn next_block(&mut self, _py: Python, index: i64, ischange: bool, minimal: bool) -> PyResult<crate::elements::PrimitiveBlock> {
        let fb = osmquadtree::pbfformat::read_file_block(&mut self.fbuf)?;
        if fb.block_type == "OSMData" {
            let bl = osmquadtree::elements::PrimitiveBlock::read(index, fb.pos, &fb.data(), ischange, minimal)?;
            Ok(crate::elements::PrimitiveBlock::new(bl))
        } else {
            Err(PyValueError::new_err(format!("block at {} not a OSMData", fb.pos)))
        }
    }
    
    pub fn read_block_at(&mut self, py: Python, index: i64, pos: u64, ischange: bool, minimal: bool) -> PyResult<crate::elements::PrimitiveBlock> {
        self.fbuf.seek(SeekFrom::Start(pos))?;
        self.next_block(py, index, ischange, minimal)
    }
}

#[pyclass]
pub struct HeaderBlock {
    inner: osmquadtree::pbfformat::HeaderBlock
}
#[pymethods]
impl HeaderBlock {
    #[getter]
    pub fn bbox(&self) -> PyResult<Vec<i64>> { Ok(self.inner.bbox.clone()) }
    
    #[getter]
    pub fn writer(&self) -> PyResult<String> { Ok(self.inner.writer.clone()) }
    
    #[getter]
    pub fn features(&self) -> PyResult<Vec<String>> { Ok(self.inner.features.clone()) }
    
    #[getter]
    pub fn index(&self, py: Python) -> PyResult<PyObject> {
        let res = PyList::empty(py);
        for ii in &self.inner.index {
            
            res.append((PyCell::new(py,crate::elements::Quadtree::new(ii.quadtree.clone()))?, ii.is_change, ii.location, ii.length))?;
        }
        Ok(res.into())
    }
}
    

/*
#[pyproto]
impl PyObjectProtocol for Count {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Count [{}, {}, {}]", self.inner.node.num, self.inner.way.num, self.inner.relation.num))
    }
}*/
        
    
    


pub(crate) fn wrap_readpbf(m: &PyModule) -> PyResult<()> {
    
    //m.add_wrapped(wrap_pyfunction!(read))?;
    m.add_class::<ReadPbfFile>()?;
    m.add_class::<FileBlock>()?;
    m.add_class::<HeaderBlock>()?;
    
    
    Ok(())
}
