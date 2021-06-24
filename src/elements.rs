use pyo3::prelude::*;
use pyo3::{wrap_pyfunction,PyObjectProtocol};
use pyo3::types::{PyBytes,PyList, PyTuple};
use pyo3::exceptions::*;
use std::sync::Arc;
//use std::ops::Drop;

#[pyclass]
pub struct Quadtree {
    inner: osmquadtree::elements::Quadtree
}

impl Quadtree {
    pub fn new(inner: osmquadtree::elements::Quadtree) -> Quadtree {
        Quadtree{inner: inner}
    }
}

#[pymethods]
impl Quadtree {
    #[getter]
    pub fn integer(&self) -> PyResult<i64> { Ok(self.inner.as_int()) }
    
    #[getter]
    pub fn tuple(&self) -> PyResult<(u32,u32,u32)> { Ok(self.inner.as_tuple().xyz()) }

    #[getter]
    pub fn string(&self) -> PyResult<String> { Ok(self.inner.as_string()) }
}

#[pyproto]
impl PyObjectProtocol for Quadtree {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    
}
#[pyclass]
pub struct PrimitiveBlock {
    inner: Arc<osmquadtree::elements::PrimitiveBlock>
}

impl PrimitiveBlock {
    pub fn new(bl: osmquadtree::elements::PrimitiveBlock) -> PrimitiveBlock {
        PrimitiveBlock{inner: Arc::new(bl)}
    }
    
    pub fn get_inner<'a>(&'a self) -> &'a osmquadtree::elements::PrimitiveBlock {
        &self.inner
    }
    
}

fn prep_which<T>(vv: &Vec<T>, mut which: i64) -> PyResult<usize> {
    let nl = vv.len() as i64;
    if which >= nl {
        return Err(PyIndexError::new_err(format!("{} >= {}",which,nl)));
    }
    if which < 0 {
        which += nl;
    }

    if which < 0 {
        return Err(PyIndexError::new_err(format!("{} >= {}",which,nl)));
    }
    Ok(which as usize)
}

#[pymethods]
impl PrimitiveBlock {
    #[getter]
    pub fn index(&self) -> PyResult<i64> { Ok(self.inner.index) }
    
    #[getter]
    pub fn location(&self) -> PyResult<u64> { Ok(self.inner.location) }
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.inner.quadtree.clone())) }
    
    
    #[getter]
    pub fn start_date(&self) -> PyResult<i64> { Ok(self.inner.start_date) }
    
    #[getter]
    pub fn end_date(&self) -> PyResult<i64> { Ok(self.inner.end_date) }
    
    pub fn num_nodes(&self) -> PyResult<i64> { Ok(self.inner.nodes.len() as i64) }
    pub fn num_ways(&self) -> PyResult<i64> { Ok(self.inner.ways.len() as i64) }
    pub fn num_relations(&self) -> PyResult<i64> { Ok(self.inner.relations.len() as i64) }
    
    pub fn node_at(&self, which: i64) -> PyResult<Node> {
            
        Ok(Node{inner: self.inner.clone(), which: prep_which(&self.inner.nodes, which)?})
    }
    pub fn way_at(&self, which: i64) -> PyResult<Way> {
        
            
        Ok(Way{inner: self.inner.clone(), which: prep_which(&self.inner.ways, which)?})
    }
    pub fn relation_at(&self, which: i64) -> PyResult<Relation> {
        
        Ok(Relation{inner: self.inner.clone(), which: prep_which(&self.inner.relations, which)?})
        
    }
    
    pub fn node_tuple_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        let n = &self.inner.nodes[prep_which(&self.inner.nodes, which)?];
        
        prep_node_tuple(py, n)
        
    }
    pub fn way_tuple_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        let n = &self.inner.ways[prep_which(&self.inner.ways, which)?];
        
        prep_way_tuple(py, n)
        
    }
    pub fn relation_tuple_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        let n = &self.inner.relations[prep_which(&self.inner.relations, which)?];
        
        prep_relation_tuple(py, n)
        
    }
    
}
#[pyproto]
impl PyObjectProtocol for PrimitiveBlock {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PrimitiveBlock {}", self.inner.index))
    }
}
/*
impl Drop for PrimitiveBlock {
    fn drop(&mut self) {
        println!("drop PrimitiveBlock {}", self.inner.index)
    }
}*/

fn prep_tags(py: Python, tgs: &Vec<osmquadtree::elements::Tag>) -> PyResult<PyObject> {
    let res = PyList::empty(py);
    for t in tgs {
        res.append((t.key.clone(), t.val.clone()))?;
    }
    Ok(res.into())
}

fn prep_info(py: Python, info: &osmquadtree::elements::Info) -> PyResult<PyObject> {
    let mut res = Vec::new();
    res.push(info.version.into_py(py));
    res.push(info.changeset.into_py(py));
    res.push(info.timestamp.into_py(py));
    res.push(info.user.clone().into_py(py));
    res.push(info.user_id.into_py(py));
    Ok(PyTuple::new(py,res).into())
}

fn prep_node_tuple(py: Python, n: &osmquadtree::elements::Node) -> PyResult<PyObject> {
     
    let mut v = Vec::new();
    v.push("node".into_py(py));
    v.push(n.id.into_py(py));
    match &n.info {
        Some(i) => {
            v.push(prep_info(py, &i)?);
        },
        None => {
            v.push(py.None());
        }
    }
    
    v.push(prep_tags(py, &n.tags)?);
    v.push(n.lon.into_py(py));
    v.push(n.lat.into_py(py));
    v.push(Quadtree::new(n.quadtree).into_py(py));
    
    Ok(PyTuple::new(py,v).into())
}
fn prep_way_tuple(py: Python, n: &osmquadtree::elements::Way) -> PyResult<PyObject> {
     
    let mut v = Vec::new();
    v.push("way".into_py(py));
    v.push(n.id.into_py(py));
    match &n.info {
        Some(i) => {
            v.push(prep_info(py, &i)?);
        },
        None => {
            v.push(py.None());
        }
    }
    
    v.push(prep_tags(py, &n.tags)?);
    v.push(n.refs.clone().into_py(py));
    
    v.push(Quadtree::new(n.quadtree).into_py(py));
    
    Ok(PyTuple::new(py,v).into())
}
fn prep_relation_tuple(py: Python, n: &osmquadtree::elements::Relation) -> PyResult<PyObject> {
     
    let mut v = Vec::new();
    v.push("relation".into_py(py));
    v.push(n.id.into_py(py));
    match &n.info {
        Some(i) => {
            v.push(prep_info(py, &i)?);
        },
        None => {
            v.push(py.None());
        }
    }
    
    v.push(prep_tags(py, &n.tags)?);
    v.push(prep_mems(py, &n.members)?);
    v.push(Quadtree::new(n.quadtree).into_py(py));
    
    Ok(PyTuple::new(py,v).into())
}

#[pyclass]
pub struct Node {
    inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    which: usize,
}

impl Node {
    fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Node {
        &self.inner.nodes[self.which]
    }
    
    fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}

#[pymethods]
impl Node {
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    #[getter]
    pub fn changetype(&self) -> PyResult<String> {
        Ok(changetype_str(&self.get_ele().changetype))
    }
    
    #[getter]
    pub fn version(&self) -> PyResult<i64> { Ok(self.get_info()?.version) }
    
    #[getter]
    pub fn timestamp(&self) -> PyResult<i64> { Ok(self.get_info()?.timestamp) }
    
    #[getter]
    pub fn changeset(&self) -> PyResult<i64> { Ok(self.get_info()?.changeset) }
    
    #[getter]
    pub fn user_id(&self) -> PyResult<i64> { Ok(self.get_info()?.user_id) }
    
    #[getter]
    pub fn user(&self) -> PyResult<String> { Ok(self.get_info()?.user.clone()) }
    
    

    #[getter]
    pub fn tags(&self, py: Python) -> PyResult<PyObject> { prep_tags(py, &self.get_ele().tags) }
    
    #[getter]
    pub fn lon(&self) -> PyResult<i32> { Ok(self.get_ele().lon) }
    
    #[getter]
    pub fn lat(&self) -> PyResult<i32> { Ok(self.get_ele().lat) }

    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
}


#[pyproto]
impl PyObjectProtocol for Node {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Node {}", self.get_ele().id))
    }
}
#[pyclass]
pub struct Way {
    inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    which: usize,
}

impl Way {
    fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Way {
        &self.inner.ways[self.which]
    }
    
    fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}


#[pymethods]
impl Way {
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    #[getter]
    pub fn changetype(&self) -> PyResult<String> {
        Ok(changetype_str(&self.get_ele().changetype))
    }
    #[getter]
    pub fn version(&self) -> PyResult<i64> { Ok(self.get_info()?.version) }
    
    #[getter]
    pub fn timestamp(&self) -> PyResult<i64> { Ok(self.get_info()?.timestamp) }
    
    #[getter]
    pub fn changeset(&self) -> PyResult<i64> { Ok(self.get_info()?.changeset) }
    
    #[getter]
    pub fn user_id(&self) -> PyResult<i64> { Ok(self.get_info()?.user_id) }
    
    #[getter]
    pub fn user(&self) -> PyResult<String> { Ok(self.get_info()?.user.clone()) }
    
    

    #[getter]
    pub fn tags(&self, py: Python) -> PyResult<PyObject> { prep_tags(py, &self.get_ele().tags) }
    
    #[getter]
    pub fn refs(&self) -> PyResult<Vec<i64>> { Ok(self.get_ele().refs.clone()) }
    
    

    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
}
#[pyproto]
impl PyObjectProtocol for Way {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Way {}", self.get_ele().id))
    }

    
}


#[pyclass]
pub struct Relation {
    inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    which: usize,
}

impl Relation {
    fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Relation {
        &self.inner.relations[self.which]
    }
    fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}
fn mem_role_str(e: &osmquadtree::elements::ElementType) -> String {
    match e {
        osmquadtree::elements::ElementType::Node => String::from("node"),
        osmquadtree::elements::ElementType::Way => String::from("way"),
        osmquadtree::elements::ElementType::Relation => String::from("relation")
    }
}
fn changetype_str(e: &osmquadtree::elements::Changetype) -> String {
    match e {
        osmquadtree::elements::Changetype::Normal => String::from("normal"),
        osmquadtree::elements::Changetype::Delete => String::from("delete"),
        osmquadtree::elements::Changetype::Remove => String::from("remove"),
        osmquadtree::elements::Changetype::Modify => String::from("modify"),
        osmquadtree::elements::Changetype::Unchanged => String::from("unchanged"),
        osmquadtree::elements::Changetype::Create => String::from("create"),
        
    }
}
fn prep_mems(py: Python, mems: &[osmquadtree::elements::Member]) -> PyResult<PyObject> {
    let res = PyList::empty(py);
    for m in mems {
        res.append((mem_role_str(&m.mem_type), m.mem_ref, m.role.clone()))?;
    
    }
    Ok(res.into())
}

#[pymethods]
impl Relation {
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    #[getter]
    pub fn changetype(&self) -> PyResult<String> {
        Ok(changetype_str(&self.get_ele().changetype))
    }
    
    #[getter]
    pub fn version(&self) -> PyResult<i64> { Ok(self.get_info()?.version) }
    
    #[getter]
    pub fn timestamp(&self) -> PyResult<i64> { Ok(self.get_info()?.timestamp) }
    
    #[getter]
    pub fn changeset(&self) -> PyResult<i64> { Ok(self.get_info()?.changeset) }
    
    #[getter]
    pub fn user_id(&self) -> PyResult<i64> { Ok(self.get_info()?.user_id) }
    
    #[getter]
    pub fn user(&self) -> PyResult<String> { Ok(self.get_info()?.user.clone()) }
    
    

    #[getter]
    pub fn tags(&self, py: Python) -> PyResult<PyObject> { prep_tags(py, &self.get_ele().tags) }
    
    #[getter]
    pub fn members(&self, py: Python) -> PyResult<PyObject> { 
        prep_mems(py, &self.get_ele().members)
        
    }
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
}
#[pyproto]
impl PyObjectProtocol for Relation {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Relation {}", self.get_ele().id))
    }
}

#[pyfunction]
pub fn read_primitive_block(index: i64, location: u64, data: &PyBytes, ischange: bool, minimal: bool) -> PyResult<PrimitiveBlock> {
    
    let bl = osmquadtree::elements::PrimitiveBlock::read(index, location, data.as_bytes(), ischange,minimal)?;
    Ok(PrimitiveBlock{inner: Arc::new(bl)})
}
    

pub(crate) fn wrap_elements(m: &PyModule) -> PyResult<()> {
    
    m.add_wrapped(wrap_pyfunction!(read_primitive_block))?;
    m.add_class::<Node>()?;
    m.add_class::<Way>()?;
    m.add_class::<Relation>()?;
    m.add_class::<PrimitiveBlock>()?;
    m.add_class::<Quadtree>()?;
    
    Ok(())
}

