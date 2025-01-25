
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use osmquadtree::logging::{ProgressBytes,ProgressPercent,Messenger,TaskSequence};
use std::cell::RefCell;


pub struct ProgressBytesPython {
    obj: PyObject,
    bytes: RefCell<u64>,
    timer: RefCell<osmquadtree::utils::Timer>
}

impl ProgressBytesPython {
    pub fn new(obj: PyObject) -> Box<dyn ProgressBytes> {
        
        Box::new(ProgressBytesPython{obj: obj, bytes: RefCell::new(0), timer: RefCell::new(osmquadtree::utils::Timer::new())})
    }
}

impl ProgressBytes for ProgressBytesPython {
    
    
    fn change_message(&self, new_message: &str) {
        
        Python::with_gil(|py| { self.obj.call_method1(py, "set_message", (new_message,)).expect("!!"); });
        
        /*let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();
        
        self.obj.call_method1(py, "set_message", (new_message,)).expect("!!");*/
    }
    
    
    fn progress_bytes(&self, bytes: u64) {
        if self.timer.borrow().since()>2.0 {
            //let gil_guard = Python::acquire_gil();
            //let py = gil_guard.python();
            Python::with_gil(|py| { 
                self.obj.call_method1(py, "progress_bytes", (bytes,)).expect("!!");
            });
            self.timer.borrow_mut().reset();
        }
        *self.bytes.borrow_mut() = bytes;
        
    }
    fn finish(&self) {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        Python::with_gil(|py| { 
            self.obj.call_method1(py, "progress_bytes", (*self.bytes.borrow(),)).expect("!!");
            self.obj.call_method0(py, "finish").expect("!!");
        });
       
        
    }
}



pub struct ProgressPercentPython {
    obj: PyObject
}

impl ProgressPercentPython {
    pub fn new(obj: PyObject) -> Box<dyn ProgressPercent> {
        
        Box::new(ProgressPercentPython{obj: obj})
    }
}

impl ProgressPercent for ProgressPercentPython {
    
    
    fn change_message(&self, new_message: &str) {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        Python::with_gil(|py| { 
            self.obj.call_method1(py, "set_message", (new_message,)).expect("!!");
        });
    }
    
    
    fn progress_percent(&self, percent: f64) {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        Python::with_gil(|py| { 
            self.obj.call_method1(py, "progress_percent", (percent,)).expect("!!");
        });
        
        
    }
    fn finish(&self) {
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        Python::with_gil(|py| { 
            self.obj.call_method0(py, "finish").expect("!!");
        });
       
        
    }
}


#[pyclass]
pub struct MessengerPython {
    obj: PyObject
}    



#[pymethods]
impl MessengerPython {
    
    #[new]
    pub fn new(obj: PyObject) -> PyResult<MessengerPython> {
        
        Ok(MessengerPython{obj: obj})
    }
    
    
}

impl Messenger for MessengerPython {
    
    
    fn message(&self, message: &str) {
        
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        Python::with_gil(|py| { 
            self.obj.call_method1(py, "message", (message,)).expect("!!");
        });
    }
    
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent> {
        
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        let nobj = Python::with_gil(|py| { 
            self.obj.call_method1(py, "start_progress_percent", (message,)).expect("!!")
        });
        
        ProgressPercentPython::new(nobj)
    }
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        
        //let gil_guard = Python::acquire_gil();
        //let py = gil_guard.python();
        
        let nobj = Python::with_gil(|py| { 
            self.obj.call_method1(py, "start_progress_bytes", (message,total_bytes)).expect("!!")
        });
        
        ProgressBytesPython::new(nobj)
    }
    
    fn start_task_sequence(&self, _: &str, _: usize) -> Box<(dyn TaskSequence + 'static)> { todo!() }
        
}

#[pyfunction]
pub fn register_messenger(_py: Python, obj: PyObject) -> PyResult<()> {
    
    /*let messenger = Box::new(MessengerPython::new(obj)?);
    osmquadtree::logging::set_boxed_messenger(messenger)?;
    Ok(())*/
    
    match MessengerPython::new(obj) {
        Ok(mm) => {
            match osmquadtree::logging::set_boxed_messenger(Box::new(mm)) {
                Ok(()) => { return Ok(()); },
                Err(e) => {
                    println!("set_boxed_messenger {:?}", e);
                    return Err(e.into());
                }
            }
        },
        Err(e) => {
            println!("MessengerPython::new {:?}", e);
            return Err(e);
        }
    }
}
    

pub(crate) fn wrap_messaging(m: &Bound<'_, PyModule>) -> PyResult<()> {
    
    
    m.add_class::<MessengerPython>()?;
    m.add_wrapped(wrap_pyfunction!(register_messenger))?;
    Ok(())
}
