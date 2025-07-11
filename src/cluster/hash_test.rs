use crate::cluster::hash::hash;
use crate::probe::probe::Probe;

#[test]
fn run() {
    log::info!("from unit test")
}

#[test]
fn should_generate_hash() {
    let probe_id = Probe {
        probe_id: "PRB3422242242112300000000000000000000011111112222334445556666123131782131231231233437687687623423412".to_string(),
        event_id: "event123123".to_string(),
        event_received_time: 3423423423423,
        data: "this is an dummy data".to_string(),
    };

    assert_eq!(11564296154245411618, hash(probe_id.probe_id))
}

