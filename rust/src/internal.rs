//! Internal module that is NOT exposed to flutter_rust_bridge
//! This module holds the actual lancedb types using raw pointer type erasure

use std::sync::Arc;

/// Type-erased container for lancedb Connection using raw pointer
pub struct ConnectionHolder {
    ptr: *const (),
}

// SAFETY: lancedb::Connection is Send + Sync
unsafe impl Send for ConnectionHolder {}
unsafe impl Sync for ConnectionHolder {}

impl ConnectionHolder {
    pub fn new(conn: lancedb::Connection) -> Self {
        let boxed = Box::new(Arc::new(conn));
        Self {
            ptr: Box::into_raw(boxed) as *const (),
        }
    }

    pub fn inner(&self) -> &lancedb::Connection {
        // SAFETY: ptr was created from Box<Arc<lancedb::Connection>>
        unsafe {
            let arc_ptr = self.ptr as *const Arc<lancedb::Connection>;
            &**arc_ptr
        }
    }
}

impl Drop for ConnectionHolder {
    fn drop(&mut self) {
        // SAFETY: ptr was created from Box<Arc<lancedb::Connection>>
        unsafe {
            let _ = Box::from_raw(self.ptr as *mut Arc<lancedb::Connection>);
        }
    }
}

/// Type-erased container for lancedb Table using raw pointer
pub struct TableHolder {
    ptr: *const (),
}

// SAFETY: lancedb::Table is Send + Sync
unsafe impl Send for TableHolder {}
unsafe impl Sync for TableHolder {}

impl TableHolder {
    pub fn new(table: lancedb::Table) -> Self {
        let boxed = Box::new(table);
        Self {
            ptr: Box::into_raw(boxed) as *const (),
        }
    }

    pub fn inner(&self) -> &lancedb::Table {
        // SAFETY: ptr was created from Box<lancedb::Table>
        unsafe {
            let table_ptr = self.ptr as *const lancedb::Table;
            &*table_ptr
        }
    }
}

impl Drop for TableHolder {
    fn drop(&mut self) {
        // SAFETY: ptr was created from Box<lancedb::Table>
        unsafe {
            let _ = Box::from_raw(self.ptr as *mut lancedb::Table);
        }
    }
}

/// Type-erased container for lancedb VectorQuery using raw pointer
pub struct VectorQueryHolder {
    ptr: *const (),
}

// SAFETY: lancedb::query::VectorQuery is Send + Sync
unsafe impl Send for VectorQueryHolder {}
unsafe impl Sync for VectorQueryHolder {}

impl VectorQueryHolder {
    pub fn new(query: lancedb::query::VectorQuery) -> Self {
        let boxed = Box::new(query);
        Self {
            ptr: Box::into_raw(boxed) as *const (),
        }
    }

    pub fn inner(&self) -> &lancedb::query::VectorQuery {
        // SAFETY: ptr was created from Box<lancedb::query::VectorQuery>
        unsafe {
            let query_ptr = self.ptr as *const lancedb::query::VectorQuery;
            &*query_ptr
        }
    }

    pub fn clone_inner(&self) -> lancedb::query::VectorQuery {
        self.inner().clone()
    }
}

impl Drop for VectorQueryHolder {
    fn drop(&mut self) {
        // SAFETY: ptr was created from Box<lancedb::query::VectorQuery>
        unsafe {
            let _ = Box::from_raw(self.ptr as *mut lancedb::query::VectorQuery);
        }
    }
}
