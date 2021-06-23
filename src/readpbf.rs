use pyo3::prelude::*;
use pyo3::PyObjectProtocol;
//use pyo3::wrap_pyfunction;
use pyo3::types::{PyList,PyTuple,PyBytes};
use pyo3::exceptions::*;
//use std::sync::Arc;
use std::io::{Seek,SeekFrom,BufReader, Read};
use std::fs::File;

use channelled_callbacks::{CallFinish,CallbackMerge,CallbackSync,Callback,ReplaceNoneWithTimings,Timings,MergeTimings};

#[pyclass]
pub struct FileBlock {
    inner: osmquadtree::pbfformat::FileBlock,
}
impl FileBlock {
    pub fn new(inner: osmquadtree::pbfformat::FileBlock) -> FileBlock {
        FileBlock{inner: inner}
    }
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
    pub fn data_raw(&self, py: Python) -> PyResult<PyObject> {
        let pp = PyBytes::new(py, &self.inner.data_raw);
        Ok(pp.into())
    }
        
        
    
    #[getter]
    pub fn is_comp(&self) -> PyResult<bool> { Ok(self.inner.is_comp) }
    
    #[getter]
    pub fn data(&self, py: Python) -> PyResult<PyObject> {
        let pp = PyBytes::new(py, &self.inner.data());
        Ok(pp.into())
    }
     
    
}

#[pyclass]
struct ReadPbfFile {
    fname: String,
    fbuf: BufReader<File>,
}
impl ReadPbfFile {
    fn read_all_call(&mut self, callback_func: PyObject, numchan: usize, ischange: bool, groupby: usize) -> PyResult<usize> {
            
        
        let co = Box::new(CollectBlocksCall::new(callback_func, groupby));
    
        let mut conv: Box<dyn CallFinish<CallType = (usize, osmquadtree::pbfformat::FileBlock), ReturnType = Timings<usize>>> =
            if numchan == 0 {
                
                osmquadtree::pbfformat::make_convert_primitive_block(ischange, co)
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
        
        
        self.fbuf.seek(SeekFrom::Start(0))?;
        let mut i=0;
        for bl in osmquadtree::pbfformat::ReadFileBlocks::new(&mut self.fbuf) {
            conv.call((i,bl));
            i+=1;
        }
        let tm = conv.finish()?;
    
        let mut r = 0;
        for (_,t) in tm.others {
            r += t;
        }
        
        Ok(r)
    }
    
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
    
    
    
    pub fn next_block(&mut self, py: Python, index: i64, ischange: bool, minimal: bool) -> PyResult<PyObject> {
        let fb = osmquadtree::pbfformat::read_file_block(&mut self.fbuf)?;
        if fb.block_type == "OSMData" {
            let bl = osmquadtree::elements::PrimitiveBlock::read(index, fb.pos, &fb.data(), ischange, minimal)?;
            Ok(crate::elements::PrimitiveBlock::new(bl).into_py(py))
        } else if fb.block_type == "OSMHeader" {
            let hb = osmquadtree::pbfformat::HeaderBlock::read(fb.pos, &fb.data(), &self.fname)?;
            Ok(HeaderBlock{inner: hb}.into_py(py))
        } else {
            
            Err(PyValueError::new_err(format!("block at {} not a OSMData or OSMHeader", fb.pos)))
        }
    }
    
    pub fn read_block_at(&mut self, py: Python, index: i64, pos: u64, ischange: bool, minimal: bool) -> PyResult<PyObject> {
        self.fbuf.seek(SeekFrom::Start(pos))?;
        self.next_block(py, index, ischange, minimal)
    }
    
    pub fn read_all(&mut self, py: Python, callback_func: PyObject, numchan: usize, ischange: bool, groupby: usize) -> PyResult<usize> {
        py.allow_threads(|| self.read_all_call(callback_func, numchan, ischange, groupby))
        
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
    


    

struct CollectBlocksCall {
    callback: PyObject,
    pending: Vec<osmquadtree::elements::PrimitiveBlock>,
    groupby: usize,
    count: usize
}
impl CollectBlocksCall {
    pub fn new(callback: PyObject, groupby: usize) -> CollectBlocksCall {
        CollectBlocksCall{callback: callback, pending: Vec::new(), groupby: groupby, count: 0}
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
        
            list.append(bll.into_py(py)).expect("!!");
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
        
        if self.pending.len() >= self.groupby {
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


pub fn read_all_blocks_parallel_prog<T, U, F, Q>(
    fbufs: &mut Vec<F>,
    locs: &Vec<(Q, Vec<(usize, u64)>)>,
    mut pp: Box<T>,
    tot_size: u64,
    progress_call: Box<dyn Fn(f64)->std::io::Result<()>>
) -> U
where
    T: CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
    F: Seek + Read,
{
    

    let mut fposes = Vec::new();
    for f in fbufs.iter_mut() {
        fposes.push(osmquadtree::pbfformat::file_position(f).expect("!"));
    }
    progress_call(0.0).expect("!");
    let mut prog = 0;
    let pf = 100.0 / (tot_size as f64);
    for (j, (_, ll)) in locs.iter().enumerate() {
        let mut fbs = Vec::new();
        for (a, b) in ll {
            if fposes[*a] != *b {
                fbufs[*a]
                    .seek(SeekFrom::Start(*b))
                    .expect(&format!("failed to read {} @ {}", *a, *b));
            }

            let (x, y) = osmquadtree::pbfformat::read_file_block_with_pos(&mut fbufs[*a], *b)
                .expect(&format!("failed to read {} @ {}", *a, *b));

            fbs.push(y);
            fposes[*a] = x;
            prog += x-b;
        }
        
        progress_call((prog as f64) * pf).expect("!");
        
        pp.call((j, fbs));
    }
    if prog < tot_size {
        progress_call(100.0).expect("!");
    }
    pp.finish().expect("finish failed")
}




#[pyclass]
pub struct ReadFileBlocksParallel {
    
    prfx: String, 
    
    bbox: osmquadtree::elements::Bbox,
    poly: Option<osmquadtree::mergechanges::Poly>,
    
    
    progress_call: PyObject,
    
    fileobjs: Vec<BufReader<File>>,
    filelocs: Vec<(osmquadtree::elements::Quadtree, Vec<(usize, u64)>)>,
    tot_size: u64
}

impl ReadFileBlocksParallel {
    fn read_all_call(&mut self, callback_func: PyObject, numchan: usize, cb: Box<dyn Fn(f64)->std::io::Result<()>>) -> PyResult<usize> {
        
        let co = Box::new(CollectBlocksCall::new(callback_func,32));
        
        
        let conv: Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>>> =
            if numchan == 0 {
                
                osmquadtree::pbfformat::make_read_primitive_blocks_combine_call_all(co)
            } else {
                
                let cosp = CallbackSync::new(co, numchan);
                
                let mut convs: Vec<
                    Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>>>,
                > = Vec::new();
                for cos in cosp {
                    let cos2 = Box::new(ReplaceNoneWithTimings::new(cos));
                    convs.push(Box::new(Callback::new(
                        osmquadtree::pbfformat::make_read_primitive_blocks_combine_call_all(cos2)
                    )));
                }
                Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
            };


        
        
        
        let tm = read_all_blocks_parallel_prog(&mut self.fileobjs, &mut self.filelocs, conv, self.tot_size, cb);
        
        let mut r = 0;
        for (_,t) in tm.others {
            r += t;
        }
        
        Ok(r)
    }
}
        

#[pymethods]
impl ReadFileBlocksParallel {
    
    #[new]
    pub fn new(py: Python, prfx: &str, filter: PyObject, progress_call: PyObject) -> PyResult<ReadFileBlocksParallel> {
        
        let (bbox, poly) = read_filter(py, filter)?;
        
        let (fileobjs, filelocs, tot_size) = osmquadtree::update::get_file_locs(prfx, Some(bbox.clone()), None)?;
        
        
        
        Ok(ReadFileBlocksParallel{
            prfx: String::from(prfx), bbox: bbox, poly: poly,
            progress_call: progress_call, 
            fileobjs: fileobjs, filelocs: filelocs, tot_size: tot_size}
        )
    }
        
    
    pub fn num_blocks(&self) -> PyResult<usize> {
        Ok(self.filelocs.len())
    }
    
    pub fn fileblocks_at(&mut self, mut idx: i64) -> PyResult<(crate::elements::Quadtree,Vec<FileBlock>)> {
        
        if idx < 0 {
            idx += self.filelocs.len() as i64;
        }
        if idx < 0 || idx > self.filelocs.len() as i64 {
            return Err(PyIndexError::new_err(format!("{} out of range", idx)));
        }
        
        let mut res = Vec::new();
        for (a,b) in &self.filelocs[idx as usize].1 {
            
            self.fileobjs[*a].seek(SeekFrom::Start(*b))?;
            let (_,fb) = osmquadtree::pbfformat::read_file_block_with_pos(&mut self.fileobjs[*a], *b)?;
            res.push(FileBlock::new(fb));
        
        }
        Ok((crate::elements::Quadtree::new(self.filelocs[idx as usize].0.clone()), res))
    }
    
    pub fn read_all(&mut self, py: Python, callback_func: PyObject, numchan: usize) -> PyResult<usize> {
        let cb: Box<dyn Fn(f64)->std::io::Result<()> + Send + Sync> = if self.progress_call.is_none(py) {
            Box::new( |_: f64| -> std::io::Result<()> { Ok(()) } )
        } else {
            let pc = self.progress_call.clone();
            Box::new(move |p: f64| -> std::io::Result<()> {
                let gil_guard = Python::acquire_gil();
                let py = gil_guard.python();
                pc.call1(py, (p,))?;
                Ok(())
            })
        };
        py.allow_threads( || self.read_all_call(callback_func, numchan, cb))
    }
        
}

#[pyproto]
impl PyObjectProtocol for ReadFileBlocksParallel {
/*    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }*/
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("ReadFileBlocksParallel {} => {:?}, {:?}, {} files, {} locs, {} bytes", self.prfx, self.bbox, self.poly, self.fileobjs.len(), self.filelocs.len(), self.tot_size))
    }
}


 


pub(crate) fn wrap_readpbf(m: &PyModule) -> PyResult<()> {
    
    
    m.add_class::<ReadPbfFile>()?;
    m.add_class::<FileBlock>()?;
    m.add_class::<HeaderBlock>()?;
    m.add_class::<ReadFileBlocksParallel>()?;
    
    
    Ok(())
}
