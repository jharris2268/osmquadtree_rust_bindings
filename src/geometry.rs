//use osmquadtree_geometry::{process_geometry,OutputType};
//use osmquadtree_geometry::{PointGeometry,LinestringGeometry,SimplePolygonGeometry,ComplicatedPolygonGeometry};
use osmquadtree_geometry::{GeoJsonable,WithBounds};

use crate::elements::{Quadtree,prep_which,prep_tags};//,prep_info};
//use crate::readpbf::ReadFileBlocksParallel;

use pyo3::prelude::*;
use pyo3::{wrap_pyfunction,PyObjectProtocol};
use pyo3::exceptions::*;
use pyo3::types::PyBytes;
use std::sync::Arc;
use std::collections::BTreeMap;

#[pyclass]
pub struct GeometryBlock {
    inner: Arc<osmquadtree_geometry::GeometryBlock>
}

impl GeometryBlock {
    pub fn new(bl: osmquadtree_geometry::GeometryBlock) -> GeometryBlock {
        GeometryBlock{inner: Arc::new(bl)}
    }
    
    pub fn get_inner<'a>(&'a self) -> &'a osmquadtree_geometry::GeometryBlock {
        &self.inner
    }
    
}



#[pymethods]
impl GeometryBlock {
    
    #[getter]
    pub fn index(&self) -> PyResult<i64> { Ok(self.inner.index) }
    
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.inner.quadtree.clone())) }
    
    
    
    #[getter]
    pub fn end_date(&self) -> PyResult<i64> { Ok(self.inner.end_date) }
    
    pub fn num_points(&self) -> PyResult<i64> { Ok(self.inner.points.len() as i64) }
    pub fn num_linestrings(&self) -> PyResult<i64> { Ok(self.inner.linestrings.len() as i64) }
    pub fn num_simple_polygons(&self) -> PyResult<i64> { Ok(self.inner.simple_polygons.len() as i64) }
    pub fn num_complicated_polygons(&self) -> PyResult<i64> { Ok(self.inner.complicated_polygons.len() as i64) }
    
    pub fn point_at(&self, which: i64) -> PyResult<PointGeometry> {
        PointGeometry::as_view(self.inner.clone(), prep_which(&self.inner.points, which)?)
    }
    pub fn linestring_at(&self, which: i64) -> PyResult<LinestringGeometry> {        
        LinestringGeometry::as_view(self.inner.clone(), prep_which(&self.inner.linestrings, which)?)
    }
    pub fn simple_polygon_at(&self, which: i64) -> PyResult<SimplePolygonGeometry> {
        SimplePolygonGeometry::as_view(self.inner.clone(), prep_which(&self.inner.simple_polygons, which)?)
    }
    pub fn complicated_polygon_at(&self, which: i64) -> PyResult<ComplicatedPolygonGeometry> {
        ComplicatedPolygonGeometry::as_view(self.inner.clone(), prep_which(&self.inner.complicated_polygons, which)?)
    }
    
        
    pub fn point_geojson_at(&self, py: Python, which: i64, transform: bool) -> PyResult<PyObject> {
        let n = &self.inner.points[prep_which(&self.inner.points, which)?];
        
        Ok(wrap_json(py, &n.to_geojson(transform)?))
        
    }
    pub fn linestring_geojson_at(&self, py: Python, which: i64, transform: bool) -> PyResult<PyObject> {
        let n = &self.inner.linestrings[prep_which(&self.inner.linestrings, which)?];
        
        Ok(wrap_json(py, &n.to_geojson(transform)?))
        
    }
    pub fn simple_polygon_geojson_at(&self, py: Python, which: i64, transform: bool) -> PyResult<PyObject> {
        let n = &self.inner.simple_polygons[prep_which(&self.inner.simple_polygons, which)?];
        
        Ok(wrap_json(py, &n.to_geojson(transform)?))
        
    }
    pub fn complicated_polygon_geojson_at(&self, py: Python, which: i64, transform: bool) -> PyResult<PyObject> {
        let n = &self.inner.complicated_polygons[prep_which(&self.inner.complicated_polygons, which)?];
        
        Ok(wrap_json(py, &n.to_geojson(transform)?))
        
    }
    
    pub fn as_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        Ok(wrap_json(py, &self.inner.to_geojson(transform)?))
    }
}
#[pyproto]
impl PyObjectProtocol for GeometryBlock {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("GeometryBlock {}", self.inner.index))
    }
}

fn wrap_json(py: Python, v: &serde_json::Value) -> PyObject {
    
    match v {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => b.into_py(py),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                n.as_i64().into_py(py)
            } else if n.is_u64() {
                n.as_u64().into_py(py)
            } else {
                n.as_f64().into_py(py)
            }
        },
        serde_json::Value::String(s) => s.into_py(py),
        serde_json::Value::Array(v) => {
            let mut r = Vec::new();
            for vi in v {
                r.push(wrap_json(py,vi));
            }
            r.into_py(py)
        },
        serde_json::Value::Object(o) => {
            let mut r = BTreeMap::new();
            for (ki,vi) in o {
                r.insert(ki,wrap_json(py,vi));
            }
            r.into_py(py)
        },
    }
}       


#[derive(Clone)]
enum PointGeometryItem {
    View((Arc<osmquadtree_geometry::GeometryBlock>,usize)),
    Item(osmquadtree_geometry::PointGeometry)
}


#[pyclass]
#[derive(Clone)]
pub struct PointGeometry {
    
    inner: PointGeometryItem
    
    
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl PointGeometry {
    
    pub fn as_view(pb: Arc<osmquadtree_geometry::GeometryBlock>, which: usize) -> PyResult<PointGeometry> {
        Ok(PointGeometry{inner: PointGeometryItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree_geometry::PointGeometry) -> PyResult<PointGeometry> {
        Ok(PointGeometry{inner: PointGeometryItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::PointGeometry {
        match self.inner {
            PointGeometryItem::View((ref pb, wh)) => &pb.points[wh],
            PointGeometryItem::Item(ref nd) => &nd
        }
        //&self.inner.nodes[self.which]
    }
    
    pub fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}

fn as_tuple(ll: &osmquadtree_geometry::LonLat) -> (i32,i32) {
    (ll.lon,ll.lat)
}

fn as_xy_tuple(ll: &osmquadtree_geometry::LonLat) -> (f64,f64) {
    let p = ll.forward();
    (p.x,p.y)
}

fn prep_bounds(py: Python, p: &osmquadtree::elements::Bbox, transform: bool) -> PyResult<PyObject> {
    if transform {
        let a = osmquadtree_geometry::LonLat::new(p.minlon,p.minlat).forward();
        let b = osmquadtree_geometry::LonLat::new(p.maxlon,p.maxlat).forward();
        Ok((a.x,a.y,b.x,b.y).into_py(py))
    } else {
        Ok(p.as_tuple().into_py(py))
    }
}

#[pymethods]
impl PointGeometry {
    pub fn clone(&self) -> PyResult<PointGeometry> {
        PointGeometry::as_item(self.get_ele().clone())
    }
    
    
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    
    
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
    pub fn lonlat(&self) -> PyResult<(i32,i32)> { Ok(as_tuple(&self.get_ele().lonlat)) }
    
    #[getter]
    pub fn xy(&self) -> PyResult<(f64,f64)> { Ok(as_xy_tuple(&self.get_ele().lonlat)) }
    
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
    
    #[getter]
    pub fn layer(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().layer.clone()) }
    
    #[getter]
    pub fn minzoom(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().minzoom.clone()) }
    
    pub fn as_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        
        Ok(wrap_json(py, &self.get_ele().to_geojson(transform)?))
    }
    
    
    pub fn bounds(&self, py: Python, transform: bool) -> PyResult<PyObject> { prep_bounds(py,&self.get_ele().bounds(), transform) }
    
    pub fn to_geometry_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        Ok(wrap_json(py, &self.get_ele().to_geometry_geojson(transform)?))
    }   
    
    pub fn wkb(&self, py: Python, transform: bool, srid: bool) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &self.get_ele().to_wkb(transform, srid)?).into_py(py))
    }
    
}


#[pyproto]
impl PyObjectProtocol for PointGeometry {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Point {}", self.get_ele().id))
    }
}



fn get_ring<'a, T: Iterator<Item=&'a osmquadtree_geometry::LonLat>>(py: Python, ll: T, transform: bool) -> PyResult<PyObject> {
    if transform {
        Ok(ll.map(as_xy_tuple).collect::<Vec<(f64,f64)>>().into_py(py))
    } else {
        Ok(ll.map(as_tuple).collect::<Vec<(i32,i32)>>().into_py(py))
    }
}


#[derive(Clone)]
enum LinestringGeometryItem {
    View((Arc<osmquadtree_geometry::GeometryBlock>,usize)),
    Item(osmquadtree_geometry::LinestringGeometry)
}


#[pyclass]
#[derive(Clone)]
pub struct LinestringGeometry {
    
    inner: LinestringGeometryItem
    
    
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl LinestringGeometry {
    
    pub fn as_view(pb: Arc<osmquadtree_geometry::GeometryBlock>, which: usize) -> PyResult<LinestringGeometry> {
        Ok(LinestringGeometry{inner: LinestringGeometryItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree_geometry::LinestringGeometry) -> PyResult<LinestringGeometry> {
        Ok(LinestringGeometry{inner: LinestringGeometryItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::LinestringGeometry {
        match self.inner {
            LinestringGeometryItem::View((ref pb, wh)) => &pb.linestrings[wh],
            LinestringGeometryItem::Item(ref nd) => &nd
        }
        //&self.inner.nodes[self.which]
    }
    
    pub fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}


#[pymethods]
impl LinestringGeometry {
    pub fn clone(&self) -> PyResult<LinestringGeometry> {
        LinestringGeometry::as_item(self.get_ele().clone())
    }
    
    
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    
    
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
    pub fn lonlats(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats.iter(), false) }
    
    #[getter]
    pub fn xys(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats.iter(), true) }
    
    #[getter]
    pub fn refs(&self, py: Python) -> PyResult<PyObject> { Ok(self.get_ele().refs.clone().into_py(py)) }
    
    #[getter]
    pub fn length(&self) -> PyResult<f64> { Ok(self.get_ele().length) }
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
    
    #[getter]
    pub fn layer(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().layer.clone()) }
    
    #[getter]
    pub fn z_order(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().z_order.clone()) }
    
    #[getter]
    pub fn minzoom(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().minzoom.clone()) }
    
    pub fn as_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        
        Ok(wrap_json(py, &self.get_ele().to_geojson(transform)?))
    }
    pub fn bounds(&self, py: Python, transform: bool) -> PyResult<PyObject> { prep_bounds(py,&self.get_ele().bounds(), transform) }
    
    pub fn to_geometry_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        Ok(wrap_json(py, &self.get_ele().to_geometry_geojson(transform)?))
    } 
    pub fn wkb(&self, py: Python, transform: bool, srid: bool) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &self.get_ele().to_wkb(transform, srid)?).into_py(py))
    }
    
}


#[pyproto]
impl PyObjectProtocol for LinestringGeometry {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Linestring {}", self.get_ele().id))
    }
}


#[derive(Clone)]
enum SimplePolygonGeometryItem {
    View((Arc<osmquadtree_geometry::GeometryBlock>,usize)),
    Item(osmquadtree_geometry::SimplePolygonGeometry)
}
impl SimplePolygonGeometryItem {
    fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::SimplePolygonGeometry {
        match self {
            SimplePolygonGeometryItem::View((ref pb, wh)) => &pb.simple_polygons[*wh],
            SimplePolygonGeometryItem::Item(ref nd) => &nd
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SimplePolygonGeometry {
    
    inner: SimplePolygonGeometryItem
    
    
    //inner: Arc<osmquadtree::elements::PrimitiveBlock>,
    //which: usize,
}

impl SimplePolygonGeometry {
    
    pub fn as_view(pb: Arc<osmquadtree_geometry::GeometryBlock>, which: usize) -> PyResult<SimplePolygonGeometry> {
        Ok(SimplePolygonGeometry{inner: SimplePolygonGeometryItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree_geometry::SimplePolygonGeometry) -> PyResult<SimplePolygonGeometry> {
        Ok(SimplePolygonGeometry{inner: SimplePolygonGeometryItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::SimplePolygonGeometry {
        self.inner.get_ele()
    }
   
    
    pub fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}


#[pymethods]
impl SimplePolygonGeometry {
    pub fn clone(&self) -> PyResult<SimplePolygonGeometry> {
        SimplePolygonGeometry::as_item(self.get_ele().clone())
    }
    
    
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    
    
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
    pub fn lonlats(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats.iter(), false) }
    
    #[getter]
    pub fn xys(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats.iter(), true) }
    
    #[getter]
    pub fn refs(&self, py: Python) -> PyResult<PyObject> { Ok(self.get_ele().refs.clone().into_py(py)) }
    
    #[getter]
    pub fn area(&self) -> PyResult<f64> { Ok(self.get_ele().area) }
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
    
    #[getter]
    pub fn layer(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().layer.clone()) }
    
    #[getter]
    pub fn z_order(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().z_order.clone()) }
    
    #[getter]
    pub fn minzoom(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().minzoom.clone()) }
    
    pub fn as_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        
        Ok(wrap_json(py, &self.get_ele().to_geojson(transform)?))
    }
    pub fn bounds(&self, py: Python, transform: bool) -> PyResult<PyObject> { prep_bounds(py,&self.get_ele().bounds(), transform) }
    
    pub fn to_geometry_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        Ok(wrap_json(py, &self.get_ele().to_geometry_geojson(transform)?))
    } 
    pub fn wkb(&self, py: Python, transform: bool, srid: bool) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &self.get_ele().to_wkb(transform, srid)?).into_py(py))
    }
    
}

#[pyclass]
#[derive(Clone)]
pub struct PolygonPart {
    geom: ComplicatedPolygonGeometryItem,
    idx: usize
}

impl PolygonPart {
    fn new(geom: ComplicatedPolygonGeometryItem, idx: usize) -> PolygonPart {
        PolygonPart{geom,idx}
    }
    
    fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::PolygonPart {
        &self.geom.get_ele().parts[self.idx]
    }
}

#[pymethods]
impl PolygonPart {
    
    #[getter]
    pub fn area(&self) -> PyResult<f64> { Ok(self.get_ele().area) }
    
    #[getter]
    pub fn exterior(&self) -> PyResult<Ring> { Ok(Ring::new(self.clone(), None)) }
    
    
    pub fn num_interiors(&self) -> PyResult<usize> { Ok(self.get_ele().interiors.len()) }
    
    pub fn interior_at(&self, wh: i64) -> PyResult<Ring> { Ok(Ring::new(self.clone(), Some(prep_which(&self.get_ele().interiors, wh)?))) }
    
}

#[pyclass]
#[derive(Clone)]
pub struct Ring {
    part: PolygonPart,
    idx: Option<usize>
}

impl Ring {
    fn new(part: PolygonPart, idx: Option<usize>) -> Ring {
        Ring{part,idx}
    }
    
    fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::Ring {
        match self.idx {
            None => &self.part.get_ele().exterior,
            Some(i) => &self.part.get_ele().interiors[i]
        }
    }
}

#[pymethods]
impl Ring {
    
    #[getter]
    pub fn refs(&self, py: Python) -> PyResult<PyObject> { Ok(self.get_ele().refs()?.into_py(py)) }
    
    #[getter]
    pub fn lonlats(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats_iter(), false) }
    
    #[getter]
    pub fn xys(&self, py: Python) -> PyResult<PyObject> { get_ring(py, self.get_ele().lonlats_iter(), true) }
    
    #[getter]
    pub fn parts(&self, py: Python) -> PyResult<PyObject> {
        let mut r = Vec::new();
        for p in &self.get_ele().parts {
            r.push((p.orig_id, p.is_reversed, p.refs.clone(), get_ring(py, p.lonlats.iter(), false)?));
        }
        Ok(r.into_py(py))
    }
    
    #[getter]
    pub fn area(&self) -> PyResult<f64> { Ok(self.get_ele().area) }
    
    
}



#[derive(Clone)]
enum ComplicatedPolygonGeometryItem {
    View((Arc<osmquadtree_geometry::GeometryBlock>,usize)),
    Item(osmquadtree_geometry::ComplicatedPolygonGeometry)
}
impl ComplicatedPolygonGeometryItem {
    fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::ComplicatedPolygonGeometry {
        match self {
            ComplicatedPolygonGeometryItem::View((ref pb, wh)) => &pb.complicated_polygons[*wh],
            ComplicatedPolygonGeometryItem::Item(ref nd) => &nd
        }
    }
}


#[pyclass]
#[derive(Clone)]
pub struct ComplicatedPolygonGeometry {
    inner: ComplicatedPolygonGeometryItem
}

impl ComplicatedPolygonGeometry {
    
    pub fn as_view(pb: Arc<osmquadtree_geometry::GeometryBlock>, which: usize) -> PyResult<ComplicatedPolygonGeometry> {
        Ok(ComplicatedPolygonGeometry{inner: ComplicatedPolygonGeometryItem::View((pb.clone(),which))})
    }
    pub fn as_item(nd: osmquadtree_geometry::ComplicatedPolygonGeometry) -> PyResult<ComplicatedPolygonGeometry> {
        Ok(ComplicatedPolygonGeometry{inner: ComplicatedPolygonGeometryItem::Item(nd)})
    }
    pub fn get_ele<'a>(&'a self) -> &'a osmquadtree_geometry::ComplicatedPolygonGeometry {
        self.inner.get_ele()
    }
    
    pub fn get_info<'a>(&'a self) -> PyResult<&'a osmquadtree::elements::Info> {
        self.get_ele().info.as_ref().ok_or_else(|| PyValueError::new_err("no info present"))
    }
}


#[pymethods]
impl ComplicatedPolygonGeometry {
    pub fn clone(&self) -> PyResult<ComplicatedPolygonGeometry> {
        ComplicatedPolygonGeometry::as_item(self.get_ele().clone())
    }
    
    
    #[getter]
    pub fn id(&self) -> PyResult<i64> { Ok(self.get_ele().id) }
    
    
    
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
    
    pub fn num_parts(&self) -> PyResult<usize> { Ok(self.get_ele().parts.len()) }
    
    pub fn part_at(&self, which: i64) -> PyResult<PolygonPart> { Ok(PolygonPart::new(self.inner.clone(), prep_which(&self.get_ele().parts, which)?)) }
    
    
    #[getter]
    pub fn area(&self) -> PyResult<f64> { Ok(self.get_ele().area) }
    
    #[getter]
    pub fn quadtree(&self) -> PyResult<Quadtree> { Ok(Quadtree::new(self.get_ele().quadtree.clone())) }
    
    #[getter]
    pub fn layer(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().layer.clone()) }
    
    #[getter]
    pub fn z_order(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().z_order.clone()) }
    
    #[getter]
    pub fn minzoom(&self) -> PyResult<Option<i64>> { Ok(self.get_ele().minzoom.clone()) }
    
    pub fn as_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        
        Ok(wrap_json(py, &self.get_ele().to_geojson(transform)?))
    }
    pub fn bounds(&self, py: Python, transform: bool) -> PyResult<PyObject> { prep_bounds(py,&self.get_ele().bounds(), transform) }
    
    pub fn to_geometry_geojson(&self, py: Python, transform: bool) -> PyResult<PyObject> {
        Ok(wrap_json(py, &self.get_ele().to_geometry_geojson(transform)?))
    } 
    pub fn wkb(&self, py: Python, transform: bool, srid: bool) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &self.get_ele().to_wkb(transform, srid)?).into_py(py))
    }
}


#[pyproto]
impl PyObjectProtocol for ComplicatedPolygonGeometry {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.get_ele()))
    }
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("ComplicatedPolygon {}", self.get_ele().id))
    }
}


fn prep_style(py: Python, style_in: PyObject) -> PyResult<Arc<osmquadtree_geometry::GeometryStyle>> {
    
    if style_in.is_none(py) {
        return Ok(Arc::new(osmquadtree_geometry::GeometryStyle::default()));
    }
    
    if let Ok(style_str) = style_in.extract::<String>(py) {
        
        if std::path::Path::new(&style_str).is_file() {
        
            return Ok(Arc::new(osmquadtree_geometry::GeometryStyle::from_file(&style_str)?));
        } else {
            
            match osmquadtree_geometry::GeometryStyle::from_json(&style_str) {
                Ok(g) => { return Ok(Arc::new(g)); }
                _ => {}
            }
            
        }
    }
    
    Err(PyRuntimeError::new_err("can't handle given style argument"))
}

fn prep_minzoom(py: Python, minzoom_in: PyObject) -> PyResult<Option<osmquadtree_geometry::MinZoomSpec>> {
    
    if minzoom_in.is_none(py) { 
        Ok(None)
    } else if let Ok((ss,mz)) = minzoom_in.extract::<(String,Option<i64>)>(py) {
        if &ss == "default" {
            Ok(Some(osmquadtree_geometry::MinZoomSpec::default(5.0, mz)))
        } else {
            Ok(Some(osmquadtree_geometry::MinZoomSpec::from_reader(5.0, mz, ss.as_bytes())?))
        }
    } else {
        Err(PyRuntimeError::new_err("can't handle given minzoom argument"))
    }
}

#[pyfunction]
fn process_geometry(py: Python,
    prfx: &str,
    filter: PyObject,
    timestamp: Option<&str>,
    minzoom_in: PyObject,
    style_in: PyObject,
    numchan: usize,
) -> PyResult<Option<Vec<GeometryBlock>>> {
    
    let (_isp, bbox, _poly) = crate::readpbf::read_filter(py, filter)?;
    let ts = match timestamp {
            Some(t) => Some(osmquadtree::utils::parse_timestamp(t)?),
            None => None
        };
        
    let mut pfilelocs = osmquadtree::pbfformat::get_file_locs(prfx, Some(bbox.clone()), ts)?;
    
    let mut qq = Vec::new();
    for (p,_) in &pfilelocs.1 {
        qq.push(p.clone());
    }
    let cb = Box::new(osmquadtree_geometry::StoreBlocks::new(qq));
    
    let style = prep_style(py, style_in)?;
    
    let minzoom = prep_minzoom(py, minzoom_in)?;
    
    
    let res = py.allow_threads(|| osmquadtree_geometry::process_geometry_call(
        &mut pfilelocs,
        Some(cb),
        style,
        minzoom,
        numchan));
    
    
    
    for x in res.others {
        match x {
            (_,osmquadtree_geometry::OtherData::GeometryBlocks(gg)) => {
                let mut res2 = Vec::new();
                for (_,g) in gg {
                    res2.push(GeometryBlock::new(g));
                }
                return Ok(Some(res2));
            }
            _ => {},
        }
    }
    Ok(None)
    
    
}

#[pyfunction]
pub fn default_style(py: Python) -> PyResult<PyObject> {
    let s = osmquadtree_geometry::GeometryStyle::default();
    Ok(wrap_json(py, &serde_json::json!(s)))
}

#[pyfunction]
pub fn default_minzoom_values(_py: Python) -> PyResult<String> {
    Ok(String::from(osmquadtree_geometry::DEFAULT_MINZOOM_VALUES))
}
    
    


pub(crate) fn wrap_geometry(m: &PyModule) -> PyResult<()> {
    
    m.add_class::<PointGeometry>()?;
    m.add_class::<LinestringGeometry>()?;
    m.add_class::<LinestringGeometry>()?;
    m.add_class::<SimplePolygonGeometry>()?;
    m.add_class::<ComplicatedPolygonGeometry>()?;
    m.add_class::<PolygonPart>()?;
    m.add_class::<Ring>()?;
    m.add_class::<GeometryBlock>()?;
    m.add_wrapped(wrap_pyfunction!(process_geometry))?;
    m.add_wrapped(wrap_pyfunction!(default_style))?;
    m.add_wrapped(wrap_pyfunction!(default_minzoom_values))?;
    Ok(())
}
