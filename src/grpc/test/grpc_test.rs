use crate::grpc::node::Node;


//Run the actual server before running tests
mod tests {
    use super::*;
    use tokio::test;
    use crate::probe::probe::Probe;


    async fn setup() -> Node {
        //toto
        Node::new("http://localhost:9001".to_string()).await
    }

    #[test]
    async fn should_write_probe_data() {
        let node = setup().await;
        let result = node.write_probe_to_store(0,true,&Probe {
            probe_id: "id2".to_string(),
            event_id: "9707d6a1-61b5-11ec-9f10-0800200c9a62".to_string(),
            event_date_time: 1699082509235,
            data: "some random data".to_string(),
        }).await;
        let response = result.unwrap();
        // assert_eq!(true, response.confirmation);
    }

    #[test]
    async fn should_read_probe_data() {
        let node = setup().await;
        let result = node.read_remote_store("id2".to_string()).await;
        let response = result.unwrap().into_inner();
        assert_eq!("id2", response.probe_id);
        assert_eq!("9707d6a1-61b5-11ec-9f10-0800200c9a62", response.event_id);
        assert_eq!(1699082509235, response.event_date_time);
        assert_eq!("some random data", response.data);
    }

    #[test]
    async fn should_read_probe_data_return_not_found_status() {
        let node = setup().await;
        let result = node.read_probe_from_store(0,true,&"id3".to_string()).await;
        assert_eq!(tonic::Code::NotFound, result.unwrap_err().code());
    }
}