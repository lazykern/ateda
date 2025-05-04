use bytes::Bytes;

pub struct CutoutPacket {
    pub candid: i64,
    pub cutout_type: String,
    pub stamp_data: Bytes,
}