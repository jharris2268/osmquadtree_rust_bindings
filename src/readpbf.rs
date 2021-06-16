use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::{PyList,PyTuple};
use pyo3::exceptions::*;
//use std::sync::Arc;
use std::io::{Seek,SeekFrom,BufReader};
use std::fs::File;

use channelled_callbacks::{CallFinish,CallbackMerge,CallbackSync,Callback,ReplaceNoneWithTimings,Timings,MergeTimings};

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
    
#[pyfunction]
pub fn read_all_blocks_primitive(py: Python, fname: &str, filter: PyObject, callback: PyObject, numchan: usize) -> PyResult<usize> {
    let (bbox, poly) = read_filter(py, filter)?;
    println!("{:?} {:?}", bbox, poly);
    
    if numchan == 0 {
        read_all_blocks_primitive_call(fname, bbox, poly, callback, numchan)
    } else {
        py.allow_threads(|| read_all_blocks_primitive_call(fname, bbox, poly, callback, numchan))
    }
}

struct CollectBlocksCall {
    callback: PyObject,
    pending: Vec<osmquadtree::elements::PrimitiveBlock>,
    count: usize
}
impl CollectBlocksCall {
    pub fn new(callback: PyObject) -> CollectBlocksCall {
        CollectBlocksCall{callback: callback, pending: Vec::new(), count: 0}
    }
    
    fn clear_pending(&mut self) {
        
        if self.pending.is_empty() {
            return;
        }
        
        
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();
        
        let list = PyList::empty(py);
        let mut num=0;
        for bl in std::mem::replace(&mut self.pending, Vec::new()) {
            let bll = crate::elements::PrimitiveBlock::new(bl);
        
            list.append(PyCell::new(py,bll).expect("!")).expect("!!");
            num+=1;
        }
        
        let args = PyTuple::new(py, &[list]);
        
        self.callback.call1(py, args).expect("!!");
        self.count+=num;
    }
    
}
impl CallFinish for CollectBlocksCall {
    type CallType = osmquadtree::elements::PrimitiveBlock;
    type ReturnType = Timings<usize>;
    
    fn call(&mut self, bl: osmquadtree::elements::PrimitiveBlock) {
        self.pending.push(bl);
        
        if self.pending.len() >= 32 {
            self.clear_pending();
        }
        
        
    }
    
    fn finish(&mut self) -> std::io::Result<Timings<usize>> {
        self.clear_pending();
        
        let mut tm = Timings::new();
        tm.add_other("CollectBlocksCall", self.count);
        Ok(tm)
    }
}
        

pub fn read_filter(py: Python, filter: PyObject) -> PyResult<(osmquadtree::elements::Bbox, Option<osmquadtree::mergechanges::Poly>)> {
    if filter.is_none(py) {
        return Ok((osmquadtree::elements::Bbox::planet(), None)); 
    }
            
    let v1: PyResult<Vec<i32>> = filter.extract(py);
    match v1 {
        Ok(vv) => {
            if vv.len()!=4 {
                return Err(PyValueError::new_err("must be length 4"));
            }
            let bx = osmquadtree::elements::Bbox::new(vv[0], vv[1], vv[2], vv[3]);
            return Ok((bx, None));
        },
        Err(_) => {}
    }
    
    let v2: PyResult<String> = filter.extract(py);
    match v2 {
        Ok(filtername) => {
            let poly = osmquadtree::mergechanges::Poly::from_file(&filtername)?;
            let bbox = poly.bounds();
            return Ok((bbox, Some(poly)));
        },
        Err(_) => {}
    }
    
    let v3: PyResult<(Vec<f64>,Vec<f64>)> = filter.extract(py);
    match v3 {
        Ok((xx, yy)) => {
            if xx.len()<3 || xx.len() != yy.len() {
                return Err(PyValueError::new_err("filter must have equal xx and yy"));
            }
            let poly = osmquadtree::mergechanges::Poly::new(xx,yy);
            let bbox = poly.bounds();
            return Ok((bbox, Some(poly)));
        },
        Err(_) => {}
    }
    
    return Err(PyValueError::new_err("can't handle filter"));
    
    
}


fn read_all_blocks_primitive_call(fname: &str, _bbox: osmquadtree::elements::Bbox, _poly: Option<osmquadtree::mergechanges::Poly>, callback: PyObject, numchan: usize) -> PyResult<usize> {
    
    
    
    
    let co = Box::new(CollectBlocksCall::new(callback));
    
    let conv: Box<dyn CallFinish<CallType = (usize, osmquadtree::pbfformat::FileBlock), ReturnType = Timings<usize>>> =
        if numchan == 0 {
            
            osmquadtree::pbfformat::make_convert_primitive_block(false, co)
        } else {
            
            let cosp = CallbackSync::new(co, numchan);
            
            let mut convs: Vec<
                Box<dyn CallFinish<CallType = (usize, osmquadtree::pbfformat::FileBlock), ReturnType = Timings<usize>>>,
            > = Vec::new();
            for cos in cosp {
                let cos2 = Box::new(ReplaceNoneWithTimings::new(cos));
                convs.push(Box::new(Callback::new(
                    osmquadtree::pbfformat::make_convert_primitive_block(false, cos2)
                )));
            }
            Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
        };

    let (tm, _) = osmquadtree::pbfformat::read_all_blocks(fname, conv);
    
    let mut r = 0;
    for (_,t) in tm.others {
        r += t;
    }
    
    Ok(r)
}
    


pub(crate) fn wrap_readpbf(m: &PyModule) -> PyResult<()> {
    
    m.add_wrapped(wrap_pyfunction!(read_all_blocks_primitive))?;
    m.add_class::<ReadPbfFile>()?;
    m.add_class::<FileBlock>()?;
    m.add_class::<HeaderBlock>()?;
    
    
    Ok(())
}
