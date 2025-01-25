use pyo3::prelude::*;
//use pyo3::PyObjectProtocol;
use pyo3::types::{PyList,PyTuple,PyBytes};
use pyo3::exceptions::*;
use std::sync::Arc;
use std::io::{Seek,SeekFrom,BufReader};
use std::fs::File;

use channelled_callbacks::{CallFinish,CallbackMerge,CallbackSync,Callback,ReplaceNoneWithTimings,Timings,MergeTimings, Result as ccResult};
use osmquadtree::utils::Error;
use crate::ErrorWrapped;

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
    pub fn compression_type(&self) -> PyResult<String> { Ok(compression_type_string(self.inner.compression_type)) }
    
    #[getter]
    pub fn data(&self, py: Python) -> PyResult<PyObject> {
        let pp = PyBytes::new(py, &self.inner.data());
        Ok(pp.into())
    }
     
    
}
pub fn compression_type_string(compression_type: osmquadtree::pbfformat::CompressionType) -> String {
    match compression_type {
        osmquadtree::pbfformat::CompressionType::Uncompressed => format!("Uncompressed"),
        osmquadtree::pbfformat::CompressionType::Zlib => format!("Zlib"),
        osmquadtree::pbfformat::CompressionType::Brotli => format!("Brotli"),
        osmquadtree::pbfformat::CompressionType::Lzma => format!("Lzma"),
        osmquadtree::pbfformat::CompressionType::ZlibLevel(level) => format!("ZlibLevel({})",level),
        osmquadtree::pbfformat::CompressionType::BrotliLevel(level) => format!("BrotliLevel({})",level),
        osmquadtree::pbfformat::CompressionType::LzmaLevel(level) => format!("LzmaLevel({})",level),
    }
}
pub fn compression_type_from_string(input: (&str,u32)) -> PyResult<osmquadtree::pbfformat::CompressionType> {
    match input {
        ("Uncompressed",_) => Ok(osmquadtree::pbfformat::CompressionType::Uncompressed),
        ("ZlibLevel",s) => Ok(osmquadtree::pbfformat::CompressionType::ZlibLevel(s)),
        ("BrotliLevel",s) => Ok(osmquadtree::pbfformat::CompressionType::BrotliLevel(s)),
        ("LzmaLevel",s) => Ok(osmquadtree::pbfformat::CompressionType::LzmaLevel(s)),
        a => Err(PyValueError::new_err(format!("{:?} not an appropiate CompressionType input", a)))
    }
}

#[pyclass]
struct ReadFileBlocks {
    fname: String,
    fbuf: BufReader<File>,
}
impl ReadFileBlocks {
    fn read_all_call(&mut self, callback_func: PyObject, numchan: usize, ischange: bool, groupby: usize) -> PyResult<usize> {
            
        
        let co = Box::new(CollectBlocksCall::new(callback_func, groupby));
    
        let mut conv: Box<dyn CallFinish<CallType = (usize, osmquadtree::pbfformat::FileBlock), ReturnType = Timings<usize>, ErrorType=Error>> =
            if numchan == 0 {
                
                osmquadtree::pbfformat::make_convert_primitive_block(ischange, co)
            } else {
                
                let cosp = CallbackSync::new(co, numchan);
                
                let mut convs: Vec<
                    Box<dyn CallFinish<CallType = (usize, osmquadtree::pbfformat::FileBlock), ReturnType = Timings<usize>, ErrorType=Error>>,
                > = Vec::new();
                for cos in cosp {
                    let cos2 = Box::new(ReplaceNoneWithTimings::new(cos));
                    convs.push(Box::new(Callback::new(
                        osmquadtree::pbfformat::make_convert_primitive_block(ischange, cos2)
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
        
        match conv.finish() {
            Ok(tm) => {
                let mut r = 0;
                for (_,t) in tm.others {
                    r += t;
                }
                
                Ok(r)
            },
            Err(e) => Err(PyErr::from(ErrorWrapped{e:e.into()}))
        }
    }
    
}
    
#[pymethods]
impl ReadFileBlocks {
    #[new]
    fn new(fname: &str) -> PyResult<Self> {
        let fobj = File::open(fname)?;
        let fbuf = BufReader::new(fobj);        
        Ok(ReadFileBlocks{fname: String::from(fname), fbuf: fbuf})
        
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
            match osmquadtree::elements::PrimitiveBlock::read(index, fb.pos, &fb.data(), ischange, minimal) {
                Ok(bl) => Ok(crate::elements::PrimitiveBlock::new(bl).into_py(py)),
                Err(e) => Err(PyErr::from(ErrorWrapped{e:e}))
            }
        } else if fb.block_type == "OSMHeader" {
            match osmquadtree::pbfformat::HeaderBlock::read(fb.pos, &fb.data(), &self.fname) {
                Ok(hb) => Ok(HeaderBlock{inner: hb}.into_py(py)),
                Err(e) => Err(PyErr::from(ErrorWrapped{e:Error::Io(e)}))
            }
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
    pub fn writingprogram(&self) -> PyResult<Option<String>> { Ok(self.inner.writingprogram.clone()) }
    
    #[getter]
    pub fn source(&self) -> PyResult<Option<String>> { Ok(self.inner.source.clone()) }
    
    
    #[getter]
    pub fn optional_features(&self) -> PyResult<Vec<String>> { Ok(self.inner.optional_features.clone()) }
    
    #[getter]
    pub fn required_features(&self) -> PyResult<Vec<String>> { Ok(self.inner.required_features.clone()) }
    
    #[getter]
    pub fn index(&self, py: Python) -> PyResult<PyObject> {
        let res = PyList::empty(py);
        for ii in &self.inner.index {
            let q = crate::elements::Quadtree::new(ii.quadtree.clone());
            res.append((q, ii.is_change, ii.location, ii.length));
                       
            //res.append((PyCell::new(py,crate::elements::Quadtree::new(ii.quadtree.clone()))?, ii.is_change, ii.location, ii.length))?;
        }
        Ok(res.into())
    }
    
    #[getter]
    pub fn osmosis_replication_timestamp(&self) -> PyResult<Option<i64>> { Ok(self.inner.osmosis_replication_timestamp) }
    
    #[getter]
    pub fn osmosis_replication_sequence_number(&self) -> PyResult<Option<i64>> { Ok(self.inner.osmosis_replication_sequence_number) }
    
    #[getter]
    pub fn osmosis_replication_base_url(&self) -> PyResult<Option<String>> { Ok(self.inner.osmosis_replication_base_url.clone()) }
    
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
        
        Python::with_gil(|py| {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
            let list = PyList::empty(py);
            let mut num=0;
            for bl in std::mem::replace(&mut self.pending, Vec::new()) {
                let bll = crate::elements::PrimitiveBlock::new(bl);
            
                list.append(bll.into_py(py)).expect("!!");
                num+=1;
            }
            
            let args = PyTuple::new(py, &[list]).unwrap();
            
            self.callback.call1(py, args).expect("!!");
            self.count+=num;
        });
    }
    
}
impl CallFinish for CollectBlocksCall {
    type CallType = osmquadtree::elements::PrimitiveBlock;
    type ReturnType = Timings<usize>;
    type ErrorType = Error;
    
    fn call(&mut self, bl: osmquadtree::elements::PrimitiveBlock) {
        self.pending.push(bl);
        
        if self.pending.len() >= self.groupby {
            self.clear_pending();
        }
        
        
    }
    
    fn finish(&mut self) -> ccResult<Self::ReturnType, Error> {
        self.clear_pending();
        
        let mut tm = Timings::new();
        tm.add_other("CollectBlocksCall", self.count);
        Ok(tm)
    }
}
    
struct CollectBlocksMinimalCall {
    callback: PyObject,
    pending: Vec<osmquadtree::elements::MinimalBlock>,
    groupby: usize,
    count: usize
}
impl CollectBlocksMinimalCall {
    pub fn new(callback: PyObject, groupby: usize) -> CollectBlocksMinimalCall {
        CollectBlocksMinimalCall{callback: callback, pending: Vec::new(), groupby: groupby, count: 0}
    }
    
    fn clear_pending(&mut self) {
        
        if self.pending.is_empty() {
            return;
        }
        
        Python::with_gil(|py| {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
            let list = PyList::empty(py);
            let mut num=0;
            for bl in std::mem::replace(&mut self.pending, Vec::new()) {
                let bll = crate::elements::MinimalBlock::new(bl);
            
                list.append(bll.into_py(py)).expect("!!");
                num+=1;
            }
            
            let args = PyTuple::new(py, &[list]).unwrap();
            
            self.callback.call1(py, args).expect("!!");
            self.count+=num;
        });
    }
    
}
impl CallFinish for CollectBlocksMinimalCall {
    type CallType = osmquadtree::elements::MinimalBlock;
    type ReturnType = Timings<usize>;
    type ErrorType = Error;
    
    fn call(&mut self, bl: osmquadtree::elements::MinimalBlock) {
        self.pending.push(bl);
        
        if self.pending.len() >= self.groupby {
            self.clear_pending();
        }
        
        
    }
    
    fn finish(&mut self) -> ccResult<Self::ReturnType, Error> {
        self.clear_pending();
        
        let mut tm = Timings::new();
        tm.add_other("CollectBlocksMinimalCall", self.count);
        Ok(tm)
    }
}
    

pub fn read_filter(py: Python, filter_in: Option<PyObject>) -> PyResult<(bool, osmquadtree::elements::Bbox, Option<osmquadtree::mergechanges::Poly>)> {
    
    match filter_in {
        None => { return Ok((true, osmquadtree::elements::Bbox::planet(), None));},
        Some(filter) => {
    
            if filter.is_none(py) {
                return Ok((true, osmquadtree::elements::Bbox::planet(), None)); 
            }
                    
            let v1: PyResult<Vec<i32>> = filter.extract(py);
            match v1 {
                Ok(vv) => {
                    if vv.len()!=4 {
                        return Err(PyValueError::new_err("must be length 4"));
                    }
                    let bx = osmquadtree::elements::Bbox::new(vv[0], vv[1], vv[2], vv[3]);
                    return Ok((bx.is_planet(), bx, None));
                },
                Err(_) => {}
            }
            
            let v2: PyResult<Poly> = filter.extract(py);
            match v2 {
                Ok(vv) => {
                    let p = vv.inner.clone();
                    
                    return Ok((false, p.bounds(), Some(p)))
                },
                Err(_) => {}
            }
            
           
            return Err(PyValueError::new_err("can't handle filter"));
        }
    }
    
    
}



#[pyclass]
pub struct ReadFileBlocksParallel {
    
    prfx: String, 
    is_planet: bool, 
    bbox: osmquadtree::elements::Bbox,
    poly: Option<osmquadtree::mergechanges::Poly>,
    
    
    callback_num_blocks: usize,
    pfilelocs: osmquadtree::pbfformat::ParallelFileLocs
}

impl ReadFileBlocksParallel {
    
    
    fn get_idset(&self, py: Python, ids: PyObject) -> PyResult<Arc<dyn osmquadtree::elements::IdSet>> {
        if ids.is_none(py) {
            return Ok(Arc::new(osmquadtree::elements::IdSetAll())); 
        }
        
        let v1: PyResult<crate::elements::IdSet> = ids.extract(py);
        match v1 {
            Ok(vv) => { return Ok(vv.inner.clone()); },
            Err(_) => {},
        }
        
        let v2: PyResult<crate::elements::IdSetSet> = ids.extract(py);
        match v2 {
            Ok(vv) => {
                let aa:Arc<dyn osmquadtree::elements::IdSet> = Arc::new(vv.inner.clone());
                return Ok(aa);
            },
            Err(_) => {},
        }
        
        Err(PyTypeError::new_err("didn't recogise ids"))
    }
    
    fn read_all_call(&mut self, callback_func: PyObject, ids: Arc<dyn osmquadtree::elements::IdSet>, numchan: usize/*, cb: Box<dyn Fn(f64)->std::io::Result<()>>*/) -> PyResult<usize> {
        
        let co = Box::new(CollectBlocksCall::new(callback_func, self.callback_num_blocks));
        
        
        let conv: Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>, ErrorType=Error>> =
            if numchan == 0 {
                
                osmquadtree::pbfformat::make_read_primitive_blocks_combine_call_all_idset(co, ids.clone(), true)
            } else {
                
                let cosp = CallbackSync::new(co, numchan);
                
                let mut convs: Vec<
                    Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>, ErrorType=Error>>,
                > = Vec::new();
                for cos in cosp {
                    let cos2 = Box::new(ReplaceNoneWithTimings::new(cos));
                    convs.push(Box::new(Callback::new(
                        osmquadtree::pbfformat::make_read_primitive_blocks_combine_call_all_idset(cos2, ids.clone(), true)
                    )));
                }
                Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
            };


        
        
        
        //let tm = read_all_blocks_parallel_prog(&mut self.pfilelocs.0, &mut self.pfilelocs.1, conv, self.pfilelocs.2, cb);
        let msg = format!("read {} blocks from {} [{} chan]", self.pfilelocs.1.len(), self.prfx, numchan);
        let tm = osmquadtree::pbfformat::read_all_blocks_parallel_with_progbar(
            &mut self.pfilelocs.0,
            &self.pfilelocs.1,
            conv,
            &msg,
            self.pfilelocs.2,
        );
        
        let mut r = 0;
        for (_,t) in tm.others {
            r += t;
        }
        
        Ok(r)
    }
    
    fn read_all_minimal_call(&mut self, callback_func: PyObject, numchan: usize/*, cb: Box<dyn Fn(f64)->std::io::Result<()>>*/) -> PyResult<usize> {
        
        let co = Box::new(CollectBlocksMinimalCall::new(callback_func, self.callback_num_blocks));
        
        
        let conv: Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>, ErrorType=Error>> =
            if numchan == 0 {
                
                osmquadtree::pbfformat::make_read_minimal_blocks_combine_call_all(co)
            } else {
                
                let cosp = CallbackSync::new(co, numchan);
                
                let mut convs: Vec<
                    Box<dyn CallFinish<CallType = (usize, Vec<osmquadtree::pbfformat::FileBlock>), ReturnType = Timings<usize>, ErrorType=Error>>,
                > = Vec::new();
                for cos in cosp {
                    let cos2 = Box::new(ReplaceNoneWithTimings::new(cos));
                    convs.push(Box::new(Callback::new(
                        osmquadtree::pbfformat::make_read_minimal_blocks_combine_call_all(cos2)
                    )));
                }
                Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
            };


        
        
        
        //let tm = read_all_blocks_parallel_prog(&mut self.pfilelocs.0, &mut self.pfilelocs.1, conv, self.pfilelocs.2, cb);
        let msg = format!("read {} blocks from {} [{} chan]", self.pfilelocs.1.len(), self.prfx, numchan);
        let tm = osmquadtree::pbfformat::read_all_blocks_parallel_with_progbar(
            &mut self.pfilelocs.0,
            &self.pfilelocs.1,
            conv,
            &msg,
            self.pfilelocs.2,
        );
        
        let mut r = 0;
        for (_,t) in tm.others {
            r += t;
        }
        
        Ok(r)
    }
    
    fn get_fileblocks_at(&mut self, mut idx: i64) -> PyResult<(osmquadtree::elements::Quadtree, Vec<osmquadtree::pbfformat::FileBlock>)> {
        if idx < 0 {
            idx += self.pfilelocs.1.len() as i64;
        }
        if idx < 0 || idx > self.pfilelocs.1.len() as i64 {
            return Err(PyIndexError::new_err(format!("{} out of range", idx)));
        }
        
        let mut res = Vec::new();
        for (a,b) in &self.pfilelocs.1[idx as usize].1 {
            
            self.pfilelocs.0[*a].seek(SeekFrom::Start(*b))?;
            let (_,fb) = osmquadtree::pbfformat::read_file_block_with_pos(&mut self.pfilelocs.0[*a], *b)?;
            res.push(fb);
        
        }
        Ok((self.pfilelocs.1[idx as usize].0.clone(), res))
    }
    
    
 
}


    


#[pymethods]
impl ReadFileBlocksParallel {
    
    #[new]
    #[pyo3(signature = (prfx, filter=None,timestamp=None,callback_num_blocks=4))]
    pub fn new(py: Python, prfx: &str, filter: Option<PyObject>/*, progress_call: PyObject*/, timestamp: Option<&str>, callback_num_blocks: usize) -> PyResult<ReadFileBlocksParallel> {
        
        let (is_planet, bbox, poly) = read_filter(py, filter)?;
        
        let ts = match timestamp {
            Some(t) => Some(osmquadtree::utils::parse_timestamp(t).unwrap()),
            None => None
        };
        
        let pfilelocs = osmquadtree::pbfformat::get_file_locs(prfx, Some(bbox.clone()), ts).unwrap();
        
        
        
        Ok(ReadFileBlocksParallel{
            prfx: String::from(prfx), is_planet: is_planet, bbox: bbox, poly: poly,
            //progress_call: progress_call, 
            callback_num_blocks: callback_num_blocks,
            pfilelocs: pfilelocs}
        )
    }
        
    
    pub fn num_blocks(&self) -> PyResult<usize> {
        Ok(self.pfilelocs.1.len())
    }
    
    pub fn index_at(&self, py: Python, mut idx: i64) -> PyResult<PyObject> {
        
        if idx < 0 {
            idx += self.pfilelocs.1.len() as i64;
        }
        if idx < 0 || idx > self.pfilelocs.1.len() as i64 {
            return Err(PyIndexError::new_err(format!("{} out of range", idx)));
        }
        
        
        let s = self.pfilelocs.1[idx as usize].1.clone();
        Ok((crate::elements::Quadtree::new(self.pfilelocs.1[idx as usize].0), s).into_py(py))
    }
    
        
    
    pub fn fileblocks_at(&mut self, idx: i64) -> PyResult<(crate::elements::Quadtree,Vec<FileBlock>)> {
        
        let (q,fbs) = self.get_fileblocks_at(idx)?;
        let mut res = Vec::new();
        for fb in fbs {
            res.push(FileBlock::new(fb));
        }
        
        Ok((crate::elements::Quadtree::new(q), res))
    }
    
    pub fn primitive_block_at(&mut self, py: Python, index: i64, ids_obj: PyObject) -> PyResult<PyObject> {
        let (_,fbs) = self.get_fileblocks_at(index)?;
        let ids = self.get_idset(py, ids_obj)?;
        let merged = osmquadtree::pbfformat::read_primitive_blocks_combine(index, fbs, Some(ids.as_ref()))?;
        
        Ok(crate::elements::PrimitiveBlock::new(merged).into_py(py))
    }
    pub fn minimal_block_at(&mut self, py: Python, index: i64) -> PyResult<PyObject> {
        let (_,fbs) = self.get_fileblocks_at(index)?;
        
        let merged = osmquadtree::pbfformat::read_minimal_blocks_combine(index, fbs)?;
        
        Ok(crate::elements::MinimalBlock::new(merged).into_py(py))
    }
    
    pub fn read_all(&mut self, py: Python, callback_func: PyObject, ids_obj: PyObject, numchan: usize) -> PyResult<usize> {
        //let cb = self.get_prog_func(py);
        
        let ids = self.get_idset(py, ids_obj)?;
        py.allow_threads( || self.read_all_call(callback_func, ids, numchan))
    }
    
    pub fn read_all_minimal(&mut self, py: Python, callback_func: PyObject, numchan: usize) -> PyResult<usize> {
        //let cb = self.get_prog_func(py);
        
        py.allow_threads( || self.read_all_minimal_call(callback_func, numchan))
    }
    
    
    pub fn prep_bbox_filter(&mut self, py: Python, numchan: usize) -> PyResult<crate::elements::IdSet> {
        let ii = py.allow_threads( || osmquadtree::mergechanges::prep_bbox_filter(
            &mut self.pfilelocs,
            &self.bbox,
            &self.poly,
            numchan))?;
        
        Ok(crate::elements::IdSet::new(ii))
        
                  
    }
    
    pub fn write_merged(&mut self, py: Python, outfn: &str, ids_obj: PyObject, compression_type: (String, u32), numchan: usize) -> PyResult<()> {
        let ids = self.get_idset(py, ids_obj)?;
        
        let tx = osmquadtree::utils::LogTimes::new();
        let ct = compression_type_from_string((&compression_type.0, compression_type.1))?;
        
        py.allow_threads( || { osmquadtree::mergechanges::call_mergechanges(&mut self.pfilelocs, outfn, ids, &self.bbox, ct, tx, numchan)?; Ok(()) })
    }
    
    pub fn write_merged_sort(&mut self, py: Python, outfn: &str, ids_obj: PyObject, inmem: bool, compression_type: (String, u32), numchan: usize) -> PyResult<()> {
        
        let ids = self.get_idset(py, ids_obj)?;
        let ct = compression_type_from_string((&compression_type.0, compression_type.1))?;
        let tx = osmquadtree::utils::LogTimes::new();
        if inmem {
            
            py.allow_threads( || { osmquadtree::mergechanges::call_mergechanges_sort_inmem(&mut self.pfilelocs, outfn, ids, &self.bbox, ct, tx, numchan)?; Ok(())})
        } else {
            py.allow_threads( || { 
                let tempfn = format!("{}-temp", &outfn[..outfn.len()-4]);
                let limit = 1500000;
                let fsplit = if self.is_planet || self.pfilelocs.2 > 4 * 1024 * 1024 * 1024 {
                    128
                } else {
                    0
                };
                osmquadtree::mergechanges::call_mergechanges_sort(&mut self.pfilelocs, outfn, &tempfn, limit, fsplit, ids, &self.bbox, false, ct, tx, numchan, 8)?;
                Ok(())
            })
        }
        
        
    }
    

/*    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }*/
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("ReadFileBlocksParallel {} => {:?}, {:?}, {} files, {} locs, {} bytes", self.prfx, self.bbox, self.poly, self.pfilelocs.0.len(), self.pfilelocs.1.len(), self.pfilelocs.2))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Poly {
    inner: osmquadtree::mergechanges::Poly
}

#[pymethods]
impl Poly {
    
    #[staticmethod] 
    fn from_file(infn: &str) -> PyResult<Self> {
        Ok(Poly{inner: osmquadtree::mergechanges::Poly::from_file(infn)?})
    }
    
    #[new]
    fn new(vertsx: Vec<f64>, vertsy: Vec<f64>, name: String) -> PyResult<Self> {
        Ok(Poly{inner: osmquadtree::mergechanges::Poly::new(vertsx, vertsy,name)})
    }
    
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.inner.name.clone())
    }
    
    #[getter]
    fn vertsx(&self) -> PyResult<Vec<f64>> {
        Ok(self.inner.vertsx.clone())
    }
    
    #[getter]
    fn vertsy(&self) -> PyResult<Vec<f64>> {
        Ok(self.inner.vertsy.clone())
    }
    
    fn bounds(&self) -> PyResult<(i32,i32,i32,i32)> {
        let b = self.inner.bounds();
        Ok((b.minlon, b.minlat, b.maxlon, b.maxlat))
    }
    
    fn check_box(&self, b: (i32,i32,i32,i32)) -> PyResult<bool> {
        Ok(self.inner.check_box(&osmquadtree::elements::Bbox::new(b.0,b.1,b.2,b.3)))
    }
    
    fn contains_point(&self, ln: i32, lt: i32) -> PyResult<bool> {
        Ok(self.inner.contains_point(ln,lt)) 
    }

/*    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }*/
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
}


pub(crate) fn wrap_readpbf(m: &Bound<'_, PyModule>) -> PyResult<()> {
    
    
    m.add_class::<ReadFileBlocks>()?;
    m.add_class::<FileBlock>()?;
    m.add_class::<HeaderBlock>()?;
    m.add_class::<ReadFileBlocksParallel>()?;
    m.add_class::<Poly>()?;
    
    Ok(())
}     

