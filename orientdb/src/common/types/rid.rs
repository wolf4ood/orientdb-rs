/// Document ID
#[derive(Debug, Clone, PartialEq)]
pub struct ORecordID {
    pub cluster: i16,
    pub position: i64,
}

impl ORecordID {
    pub fn new(cluster: i16, position: i64) -> ORecordID {
        ORecordID { cluster, position }
    }
    pub fn empty() -> ORecordID {
        ORecordID {
            cluster: -1,
            position: -1,
        }
    }
}
