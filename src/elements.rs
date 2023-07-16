use pyo3::prelude::*;
use pyo3::{wrap_pyfunction,PyObjectProtocol};
use pyo3::types::{PyBytes,PyList, PyTuple};
use pyo3::exceptions::*;
use pyo3::sequence::PySequenceProtocol;
use pyo3::basic::CompareOp;
use std::sync::Arc;
//use std::ops::Drop;

#[pyclass]
#[derive(Clone)]
pub struct Quadtree {
    pub inner: osmquadtree::elements::Quadtree
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
    
    pub fn depth(&self) -> PyResult<usize> { Ok(self.inner.depth()) }
    pub fn round(&self, nd: usize) -> PyResult<Quadtree> { Ok(Quadtree::new(self.inner.round(nd))) }
    pub fn is_parent(&self, o: &Quadtree) -> PyResult<bool> { Ok(self.inner.is_parent(&o.inner)) }
    pub fn as_bbox(&self, b: f64) -> PyResult<(i32,i32,i32,i32)> { 
        let bx = self.inner.as_bbox(b);
        Ok((bx.minlon,bx.minlat,bx.maxlon,bx.maxlat))
    }
    
    
}

#[pyproto]
impl PyObjectProtocol for Quadtree {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Quadtree {}", self.inner.as_int()))
    }
    
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    
    fn __richcmp__(&self, other: Quadtree, compareop: CompareOp) -> PyResult<bool> {
        match compareop {
            CompareOp::Lt => { Ok(self.inner.as_int() < other.integer()?) },
            CompareOp::Le => { Ok(self.inner.as_int() <= other.integer()?) },
            CompareOp::Eq => { Ok(self.inner.as_int() == other.integer()?) },
            CompareOp::Ne => { Ok(self.inner.as_int() != other.integer()?) },
            CompareOp::Gt => { Ok(self.inner.as_int() > other.integer()?) },
            CompareOp::Ge => { Ok(self.inner.as_int() >= other.integer()?) },
        }
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

pub(crate) fn prep_which<T>(vv: &Vec<T>, mut which: i64) -> PyResult<usize> {
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
    
    #[new]
    pub fn from_elements(py: Python, index: i64, location: u64, quadtree: &Quadtree, start_date: i64, end_date: i64, elements: Vec<PyObject>) -> PyResult<PrimitiveBlock> {
        
        let mut pb = osmquadtree::elements::PrimitiveBlock::new(index, location);
        pb.quadtree = quadtree.inner.clone();
        pb.start_date = start_date;
        pb.end_date = end_date;
        
        for e in &elements {
            || -> PyResult<()> {
                let v1: PyResult<Node> = e.extract(py);
                match v1 {
                    Ok(ref n) => { pb.nodes.push(n.get_ele().clone()); return Ok(()); },
                    Err(_) => { }
                }
                let v2: PyResult<Way> = e.extract(py);
                match v2 {
                    Ok(ref n) => { pb.ways.push(n.get_ele().clone()); return Ok(()); },
                    Err(_) => { }
                }
                let v3: PyResult<Relation> = e.extract(py);
                match v3 {
                    Ok(ref n) => { pb.relations.push(n.get_ele().clone()); return Ok(()); },
                    Err(_) => { }
                }
                Err(PyValueError::new_err("unexpected type"))
            }()?;
        }
        Ok(PrimitiveBlock{inner: Arc::new(pb) })
    }
            
    
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
        
        Node::as_view(self.inner.clone(), prep_which(&self.inner.nodes, which)?)
        
        //Ok(Node{inner: self.inner.clone(), which: prep_which(&self.inner.nodes, which)?})
    }
    pub fn way_at(&self, which: i64) -> PyResult<Way> {
        
        Way::as_view(self.inner.clone(), prep_which(&self.inner.ways, which)?)
        //Ok(Way{inner: self.inner.clone(), which: prep_which(&self.inner.ways, which)?})
    }
    pub fn relation_at(&self, which: i64) -> PyResult<Relation> {
        
        Relation::as_view(self.inner.clone(), prep_which(&self.inner.relations, which)?)
        //Ok(Relation{inner: self.inner.clone(), which: prep_which(&self.inner.relations, which)?})
        
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

pub(crate) fn prep_tags(py: Python, tgs: &Vec<osmquadtree::elements::Tag>) -> PyResult<PyObject> {
    let res = PyList::empty(py);
    for t in tgs {
        res.append((t.key.clone(), t.val.clone()))?;
    }
    Ok(res.into())
}

pub(crate) fn prep_info(py: Python, info_op: &Option<osmquadtree::elements::Info>) -> PyResult<PyObject> {
    match info_op {
        Some(info) => {
            
            let mut res = Vec::new();
            res.push(info.version.into_py(py));
            res.push(info.changeset.into_py(py));
            res.push(info.timestamp.into_py(py));
            res.push(info.user.clone().into_py(py));
            res.push(info.user_id.into_py(py));
            Ok(PyTuple::new(py,res).into())
        },
        None => Ok(py.None())
    }
}

fn prep_node_tuple(py: Python, n: &osmquadtree::elements::Node) -> PyResult<PyObject> {
    Ok((
        "node",
        changetype_str(&n.changetype),
        n.id,
        prep_info(py, &n.info)?,
        prep_tags(py, &n.tags)?,
        (n.lon, n.lat),
        Quadtree::new(n.quadtree)
    ).into_py(py))
    
}
fn prep_way_tuple(py: Python, n: &osmquadtree::elements::Way) -> PyResult<PyObject> {
    Ok((
        "way",
        changetype_str(&n.changetype),
        n.id,
        prep_info(py, &n.info)?,
        prep_tags(py, &n.tags)?,
        n.refs.clone(),
        Quadtree::new(n.quadtree)
    ).into_py(py))
    
}
fn prep_relation_tuple(py: Python, n: &osmquadtree::elements::Relation) -> PyResult<PyObject> {
    Ok((
        "relation",
        changetype_str(&n.changetype),
        n.id,
        prep_info(py, &n.info)?,
        prep_tags(py, &n.tags)?,
        prep_mems(py, &n.members)?,
        Quadtree::new(n.quadtree)
    ).into_py(py)) 
    
}

pub fn prep_element_tuple(py: Python, ele: &osmquadtree::elements::Element) -> PyResult<PyObject> {
    match ele {
        osmquadtree::elements::Element::Node(n) => prep_node_tuple(py,n),
        osmquadtree::elements::Element::Way(w) => prep_way_tuple(py,w),
        osmquadtree::elements::Element::Relation(r) => prep_relation_tuple(py,r),
        //_ => Err(PyValueError::new_err("unexpected element type"))
    }
}
        
    

#[derive(Clone)]
enum NodeItem {
    View((Arc<osmquadtree::elements::PrimitiveBlock>,usize)),
    Item(osmquadtree::elements::Node)
}


#[pyclass]
#[derive(Clone)]
pub struct Node {
    
    inner: NodeItem
    
    
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl Node {
    
    pub fn as_view(pb: Arc<osmquadtree::elements::PrimitiveBlock>, which: usize) -> PyResult<Node> {
        Ok(Node{inner: NodeItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree::elements::Node) -> PyResult<Node> {
        Ok(Node{inner: NodeItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Node {
        match self.inner {
            NodeItem::View((ref pb, wh)) => &pb.nodes[wh],
            NodeItem::Item(ref nd) => &nd
        }
        //&self.inner.nodes[self.which]
    }
    
    pub fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}

#[pymethods]
impl Node {
    pub fn clone(&self) -> PyResult<Node> {
        Node::as_item(self.get_ele().clone())
    }
    
    #[new]
    pub fn new(
        id: i64, changetype: &str,
        version: i64, timestamp: i64, changeset: i64, user_id: i64, user: &str,
        tags: Vec<(String,String)>,
        lon: i32, lat: i32, quadtree: &Quadtree) -> PyResult<Node> {
            
        let mut nd = osmquadtree::elements::Node::new(id, changetype_from_str(changetype)?);
        let mut inf = osmquadtree::elements::Info::new();
        inf.version = version;
        inf.timestamp = timestamp;
        inf.changeset = changeset;
        inf.user_id = user_id;
        inf.user = String::from(user);
        nd.info = Some(inf);
        for (k,v) in tags {
            nd.tags.push(osmquadtree::elements::Tag::new(k,v));
        }
        nd.lon = lon;
        nd.lat = lat;
        nd.quadtree = quadtree.inner.clone();
        Node::as_item(nd)
    }
    
    pub fn as_tuple(&self, py: Python) -> PyResult<PyObject> {
        prep_node_tuple(py, self.get_ele())
    }
    
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

#[derive(Clone)]
enum WayItem {
    View((Arc<osmquadtree::elements::PrimitiveBlock>,usize)),
    Item(osmquadtree::elements::Way)
}



#[pyclass]
#[derive(Clone)]
pub struct Way {
    
    inner: WayItem,
    
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl Way {
    
    pub fn as_view(pb: Arc<osmquadtree::elements::PrimitiveBlock>, which: usize) -> PyResult<Way> {
        Ok(Way{inner: WayItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree::elements::Way) -> PyResult<Way> {
        Ok(Way{inner: WayItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Way {
        match self.inner {
            WayItem::View((ref pb, wh)) => &pb.ways[wh],
            WayItem::Item(ref nd) => &nd
        }
        //&self.inner.nodes[self.which]
    }
    
    /*fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Way {
        &self.inner.ways[self.which]
    }*/
    
    fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}


#[pymethods]
impl Way {
    
    pub fn clone(&self) -> PyResult<Way> {
        Way::as_item(self.get_ele().clone())
    }
    
    #[new]
    pub fn new(
        id: i64, changetype: &str,
        version: i64, timestamp: i64, changeset: i64, user_id: i64, user: &str,
        tags: Vec<(String,String)>,
        refs: Vec<i64>, quadtree: &Quadtree) -> PyResult<Way> {
            
        let mut wy = osmquadtree::elements::Way::new(id, changetype_from_str(changetype)?);
        let mut inf = osmquadtree::elements::Info::new();
        inf.version = version;
        inf.timestamp = timestamp;
        inf.changeset = changeset;
        inf.user_id = user_id;
        inf.user = String::from(user);
        wy.info = Some(inf);
        for (k,v) in tags {
            wy.tags.push(osmquadtree::elements::Tag::new(k,v));
        }
        wy.refs=refs;
        wy.quadtree = quadtree.inner.clone();
        Way::as_item(wy)
    }
    
    pub fn as_tuple(&self, py: Python) -> PyResult<PyObject> {
        prep_way_tuple(py, self.get_ele())
    }
    
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

#[derive(Clone)]
enum RelationItem {
    View((Arc<osmquadtree::elements::PrimitiveBlock>,usize)),
    Item(osmquadtree::elements::Relation)
}


#[pyclass]
#[derive(Clone)]
pub struct Relation {
    inner: RelationItem
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl Relation {
    pub fn as_view(pb: Arc<osmquadtree::elements::PrimitiveBlock>, which: usize) -> PyResult<Relation> {
        Ok(Relation{inner: RelationItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree::elements::Relation) -> PyResult<Relation> {
        Ok(Relation{inner: RelationItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Relation {
        match self.inner {
            RelationItem::View((ref pb, wh)) => &pb.relations[wh],
            RelationItem::Item(ref nd) => &nd
        }
        //&self.inner.nodes[self.which]
    }
    
    /*fn get_ele<'a>(&'a self) -> &'a osmquadtree::elements::Relation {
        &self.inner.relations[self.which]
    }*/
    fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}
fn mem_role_str(e: &osmquadtree::elements::ElementType) -> String {
    match e {
        osmquadtree::elements::ElementType::Node => String::from("node"),
        osmquadtree::elements::ElementType::Way => String::from("way"),
        osmquadtree::elements::ElementType::Relation => String::from("relation"),
        _ => {String::from("???")}
    }
}

fn elementtype_from_str(et: &str) -> PyResult<osmquadtree::elements::ElementType> {
    match et.to_lowercase().as_str() {
        "node" | "n" => Ok(osmquadtree::elements::ElementType::Node),
        "way" | "w" => Ok(osmquadtree::elements::ElementType::Way),
        "relation" | "r" => Ok(osmquadtree::elements::ElementType::Relation),
        _ => Err(PyValueError::new_err(format!("unknown elementtype {}", et)))
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

fn changetype_from_str(ct: &str) -> PyResult<osmquadtree::elements::Changetype> {
    match ct.to_lowercase().as_str() {
        "" | "normal" | "n" => Ok(osmquadtree::elements::Changetype::Normal),
        "delete" | "d" => Ok(osmquadtree::elements::Changetype::Delete),
        "remove" | "r" => Ok(osmquadtree::elements::Changetype::Remove),
        "modify" | "m" => Ok(osmquadtree::elements::Changetype::Modify),
        "unchanged" | "u" => Ok(osmquadtree::elements::Changetype::Unchanged),
        "create" | "c" => Ok(osmquadtree::elements::Changetype::Create),
        _ => Err(PyValueError::new_err(format!("unknown changetype {}", ct)))
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
    pub fn clone(&self) -> PyResult<Relation> {
        Relation::as_item(self.get_ele().clone())
    }
    
    #[new]
    pub fn new(
        id: i64, changetype: &str,
        version: i64, timestamp: i64, changeset: i64, user_id: i64, user: &str,
        tags: Vec<(String,String)>,
        mems: Vec<(String,i64,String)>, quadtree: &Quadtree) -> PyResult<Relation> {
            
        let mut rl = osmquadtree::elements::Relation::new(id, changetype_from_str(changetype)?);
        let mut inf = osmquadtree::elements::Info::new();
        inf.version = version;
        inf.timestamp = timestamp;
        inf.changeset = changeset;
        inf.user_id = user_id;
        inf.user = String::from(user);
        rl.info = Some(inf);
        for (k,v) in tags {
            rl.tags.push(osmquadtree::elements::Tag::new(k,v));
        }
        
        for (a,b,c) in mems {
            let et = elementtype_from_str(&a)?;
            let m = osmquadtree::elements::Member{mem_type: et, mem_ref: b, role: c};
            rl.members.push(m);
        }
        rl.quadtree = quadtree.inner.clone();
        Relation::as_item(rl)
    }
    
    pub fn as_tuple(&self, py: Python) -> PyResult<PyObject> {
        prep_relation_tuple(py, self.get_ele())
    }
    
    
    
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
#[pyfunction]
pub fn read_minimal_block(index: i64, location: u64, data: &PyBytes, ischange: bool) -> PyResult<MinimalBlock> {
    
    let bl = osmquadtree::elements::MinimalBlock::read(index, location, data.as_bytes(), ischange)?;
    Ok(MinimalBlock{inner: Box::new(bl)})
}
#[pyclass]
pub struct MinimalBlock {
    inner: Box<osmquadtree::elements::MinimalBlock>,
}

impl MinimalBlock {
    pub fn new(bl: osmquadtree::elements::MinimalBlock) -> MinimalBlock {
        MinimalBlock{inner: Box::new(bl)}
    }
}

#[pymethods]
impl MinimalBlock {
    
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
    
    pub fn node_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        
        prep_minimal_node_tuple(py, &self.inner.nodes[prep_which(&self.inner.nodes, which)?])
    }
    pub fn way_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        
        prep_minimal_way_tuple(py, &self.inner.ways[prep_which(&self.inner.ways, which)?])
    }
    pub fn relation_at(&self, py: Python, which: i64) -> PyResult<PyObject> {
        
        prep_minimal_relation_tuple(py, &self.inner.relations[prep_which(&self.inner.relations, which)?])
        
    }
}


fn prep_minimal_node_tuple(py: Python, n: &osmquadtree::elements::MinimalNode) -> PyResult<PyObject> {
    
    Ok((
        "node",
        changetype_str(&n.changetype),
        n.id,
        (n.version, n.timestamp),
        py.None(),
        (n.lon, n.lat),
        Quadtree::new(n.quadtree)
    ).into_py(py))

}

fn prep_minimal_way_tuple(py: Python, n: &osmquadtree::elements::MinimalWay) -> PyResult<PyObject> {
    
    Ok((
        "way",
        changetype_str(&n.changetype),
        n.id,
        (n.version, n.timestamp),
        py.None(),
        simple_protocolbuffers::read_delta_packed_int(&n.refs_data),
        Quadtree::new(n.quadtree)
    ).into_py(py))
    
    
}
fn prep_minimal_relation_tuple(py: Python, n: &osmquadtree::elements::MinimalRelation) -> PyResult<PyObject> {
    let mut mm: Vec<(u64,i64,Option<String>)> = Vec::new();
    for (a,b) in simple_protocolbuffers::DeltaPackedInt::new(&n.refs_data).zip(
        simple_protocolbuffers::PackedInt::new(&n.types_data)) {
        
        mm.push((b, a, None));
    }
    
    Ok((
        "relation",
        changetype_str(&n.changetype),
        n.id,
        (n.version, n.timestamp),
        py.None(),
        mm,
        Quadtree::new(n.quadtree)
    ).into_py(py))
    
    
}



use osmquadtree::elements::IdSet as __IdSet;

#[pyclass]
#[derive(Clone)]
pub struct IdSetSet {
    pub inner: osmquadtree::elements::IdSetSet,
    
}

#[pymethods]
impl IdSetSet {
    
    
    #[new]
    pub fn new() -> PyResult<IdSetSet> {
        Ok(IdSetSet{inner: osmquadtree::elements::IdSetSet::new()})
    }
    
    pub fn insert(&mut self, t: &str, id: i64) -> PyResult<()> {
        
        match t.to_lowercase().as_str() {
            "n" | "node" => { self.inner.nodes.insert(id); Ok(())},
            "w" | "way" => { self.inner.ways.insert(id); Ok(())},
            "r" | "relation" => { self.inner.relations.insert(id); Ok(())},
            _ => Err(PyValueError::new_err(format!("unexpected type {} {}", t, id)))
        }
    }
    
    pub fn add_block_full(&mut self, bl: &PrimitiveBlock) -> PyResult<()> {
        for n in &bl.inner.nodes {
            self.inner.nodes.insert(n.id);
        }
        for w in &bl.inner.ways {
            self.inner.ways.insert(w.id);
        }
        for r in &bl.inner.relations {
            self.inner.relations.insert(r.id);
        }
        Ok(())
    }
    
    
    pub fn add_block_box(&mut self, bx_in: (i32,i32,i32,i32), bl: &PrimitiveBlock) -> PyResult<()> {
        let bx = osmquadtree::elements::Bbox::new(bx_in.0,bx_in.1,bx_in.2,bx_in.3);
        for n in &bl.inner.nodes {
            if bx.contains_point(n.lon, n.lat) {
                self.inner.nodes.insert(n.id);
            }
        }
        for w in &bl.inner.ways {
            if (|| {
                for n in &w.refs {
                    if self.inner.nodes.contains(n) {
                        return true;
                    }
                }
                false
            })() {
                self.inner.ways.insert(w.id);
                for n in &w.refs {
                    if !self.inner.nodes.contains(n) {
                        self.inner.exnodes.insert(*n);
                    }
                }
            }
        }
        
        for r in &bl.inner.relations {
            if (|| {
                for m in &r.members {
                    if self.inner.contains(m.mem_type.clone(), m.mem_ref) {
                        return true;
                    }
                }
                false
            })() {
                self.inner.relations.insert(r.id);
            }
        }
        Ok(())
        
    }
    pub fn add_minimal_block_full(&mut self, bl: &MinimalBlock) -> PyResult<()> {
        for n in &bl.inner.nodes {
            self.inner.nodes.insert(n.id);
        }
        for w in &bl.inner.ways {
            self.inner.ways.insert(w.id);
        }
        for r in &bl.inner.relations {
            self.inner.relations.insert(r.id);
        }
        Ok(())
    }
    
    
    pub fn add_minimal_block_box(&mut self, bx_in: (i32,i32,i32,i32), bl: &MinimalBlock) -> PyResult<()> {
        let bx = osmquadtree::elements::Bbox::new(bx_in.0,bx_in.1,bx_in.2,bx_in.3);
        for n in &bl.inner.nodes {
            if bx.contains_point(n.lon, n.lat) {
                self.inner.nodes.insert(n.id);
            }
        }
        for w in &bl.inner.ways {
            if (|| {
                for n in simple_protocolbuffers::DeltaPackedInt::new(&w.refs_data) {
                    if self.inner.nodes.contains(&n) {
                        return true;
                    }
                }
                false
            })() {
                self.inner.ways.insert(w.id);
                for n in simple_protocolbuffers::DeltaPackedInt::new(&w.refs_data) {
                    if !self.inner.nodes.contains(&n) {
                        self.inner.exnodes.insert(n);
                    }
                }
            }
        }
        
        for r in &bl.inner.relations {
            if (|| {
                for (t,i) in simple_protocolbuffers::PackedInt::new(&r.types_data).zip(
                    simple_protocolbuffers::DeltaPackedInt::new(&r.refs_data)) {
                    if self.inner.contains(osmquadtree::elements::ElementType::from_int(t),i) {
                        return true;
                    }
                }
                false
            })() {
                self.inner.relations.insert(r.id);
            }
        }
        Ok(())
        
    }
    
    fn is_exnode(&self, i: i64) -> PyResult<bool> {
        Ok(self.inner.is_exnode(i))
    }
}

#[pyproto]
impl PySequenceProtocol<'p> for IdSetSet {
    

    fn __contains__(&self, ti: (&'p str, i64)) -> PyResult<bool> {
        
        match ti.0.to_lowercase().as_str() {
            "node" | "n" => Ok(self.inner.nodes.contains(&ti.1)),
            "way" | "w" => Ok(self.inner.ways.contains(&ti.1)),
            "relation" | "r" => Ok(self.inner.relations.contains(&ti.1)),
            _ => Err(PyValueError::new_err(format!("unexpected type {} {}", ti.0, ti.1)))
        }
    }
}

#[pyproto]
impl PyObjectProtocol for IdSetSet {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("IdSetSet"))
    }
        
}

#[pyclass]
#[derive(Clone)]
pub struct IdSet {
    pub inner: Arc<dyn osmquadtree::elements::IdSet>
}
impl IdSet {
    pub fn new(inner: Arc<dyn osmquadtree::elements::IdSet>) -> IdSet {
        IdSet{inner: inner}
    }
}

#[pyproto]
impl PySequenceProtocol<'p> for IdSet {
    

    fn __contains__(&self, ti: (&'p str, i64)) -> PyResult<bool> {
        
        match ti.0.to_lowercase().as_str() {
            "node" | "n" => Ok(self.inner.contains(osmquadtree::elements::ElementType::Node, ti.1)),
            "way" | "w" => Ok(self.inner.contains(osmquadtree::elements::ElementType::Way, ti.1)),
            "relation" | "r" => Ok(self.inner.contains(osmquadtree::elements::ElementType::Relation, ti.1)),
            _ => Err(PyValueError::new_err(format!("unexpected type {} {}", ti.0, ti.1)))
        }
    }
}

#[pyproto]
impl PyObjectProtocol for IdSet {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("IdSet"))
    }
        
}

#[pyfunction]
pub fn combine_primitive(left: &PrimitiveBlock, right: &PrimitiveBlock) -> PyResult<PrimitiveBlock> {
    
    let r = osmquadtree::elements::combine_block_primitive_clone(&left.inner, &right.inner);
    Ok(PrimitiveBlock::new(r))
}
    
#[pyfunction]
pub fn apply_change_primitive(left: &PrimitiveBlock, right: &PrimitiveBlock) -> PyResult<PrimitiveBlock> {
    
    let r = osmquadtree::elements::apply_change_primitive_clone(&left.inner, &right.inner);
    Ok(PrimitiveBlock::new(r))
}

#[pyfunction]
pub fn parse_timestamp(ts: &str) -> PyResult<i64> {
    Ok(osmquadtree::utils::parse_timestamp(ts)?)    
}

#[pyfunction]
pub fn timestamp_string(ts: i64) -> PyResult<String> {
    Ok(osmquadtree::utils::timestamp_string(ts))
}

#[pyfunction]
pub fn timestamp_string_alt(ts: i64) -> PyResult<String> {
    Ok(osmquadtree::utils::timestamp_string_alt(ts))
}


#[pyfunction]
pub fn date_string(ts: i64) -> PyResult<String> {
    Ok(osmquadtree::utils::date_string(ts))
}




pub(crate) fn wrap_elements(m: &PyModule) -> PyResult<()> {
    
    m.add_wrapped(wrap_pyfunction!(read_primitive_block))?;
    m.add_wrapped(wrap_pyfunction!(read_minimal_block))?;
    m.add_wrapped(wrap_pyfunction!(apply_change_primitive))?;
    m.add_wrapped(wrap_pyfunction!(combine_primitive))?;
    m.add_class::<Node>()?;
    m.add_class::<Way>()?;
    m.add_class::<Relation>()?;
    m.add_class::<PrimitiveBlock>()?;
    m.add_class::<Quadtree>()?;
    m.add_class::<MinimalBlock>()?;
    m.add_class::<IdSet>()?;
    m.add_class::<IdSetSet>()?;
    
    m.add_wrapped(wrap_pyfunction!(parse_timestamp))?;
    m.add_wrapped(wrap_pyfunction!(timestamp_string))?;
    m.add_wrapped(wrap_pyfunction!(timestamp_string_alt))?;
    m.add_wrapped(wrap_pyfunction!(date_string))?;
    Ok(())
}

