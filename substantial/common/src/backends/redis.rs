pub struct Redis;

impl super::Backend for Redis {
    fn read_events(run_id: String) -> Option<crate::wit::backend::Records> {
        todo!("redis read events")
    }
}
