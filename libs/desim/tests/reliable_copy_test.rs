//! Simple test to verify that simulator is working.
#[cfg(test)]
mod reliable_copy_test {
    use anyhow::Result;
    use desim::executor::{self, PollSome};
    use desim::options::{Delay, NetworkOptions};
    use desim::proto::{NetEvent, NodeEvent, ReplCell};
    use desim::world::{NodeId, World};
    use desim::{node_os::NodeOs, proto::AnyMessage};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tracing::info;

    /// Disk storage trait and implementation.
    pub trait Storage<T> {
        fn flush_pos(&self) -> u32;
        fn flush(&mut self) -> Result<()>;
        fn write(&mut self, t: T);
    }

    #[derive(Clone)]
    pub struct SharedStorage<T> {
        pub state: Arc<Mutex<InMemoryStorage<T>>>,
    }

    impl<T> SharedStorage<T> {
        pub fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(InMemoryStorage::new())),
            }
        }
    }

    impl<T> Storage<T> for SharedStorage<T> {
        fn flush_pos(&self) -> u32 {
            self.state.lock().flush_pos
        }

        fn flush(&mut self) -> Result<()> {
            executor::yield_me(0);
            self.state.lock().flush()
        }

        fn write(&mut self, t: T) {
            executor::yield_me(0);
            self.state.lock().write(t);
        }
    }

    pub struct InMemoryStorage<T> {
        pub data: Vec<T>,
        pub flush_pos: u32,
    }

    impl<T> InMemoryStorage<T> {
        pub fn new() -> Self {
            Self {
                data: Vec::new(),
                flush_pos: 0,
            }
        }

        pub fn flush(&mut self) -> Result<()> {
            self.flush_pos = self.data.len() as u32;
            Ok(())
        }

        pub fn write(&mut self, t: T) {
            self.data.push(t);
        }
    }

    /// Server implementation.
    pub fn run_server(os: NodeOs, mut storage: Box<dyn Storage<u32>>) {
        info!("started server");

        let node_events = os.node_events();
        let mut epoll_vec: Vec<Box<dyn PollSome>> = vec![Box::new(node_events.clone())];
        let mut sockets = vec![];

        loop {
            let index = executor::epoll_chans(&epoll_vec, -1).unwrap();

            if index == 0 {
                let node_event = node_events.must_recv();
                info!("got node event: {:?}", node_event);
                if let NodeEvent::Accept(tcp) = node_event {
                    tcp.send(AnyMessage::Just32(storage.flush_pos()));
                    epoll_vec.push(Box::new(tcp.recv_chan()));
                    sockets.push(tcp);
                }
                continue;
            }

            let recv_chan = sockets[index - 1].recv_chan();
            let socket = &sockets[index - 1];

            let event = recv_chan.must_recv();
            info!("got event: {:?}", event);
            if let NetEvent::Message(AnyMessage::ReplCell(cell)) = event {
                if cell.seqno != storage.flush_pos() {
                    info!("got out of order data: {:?}", cell);
                    continue;
                }
                storage.write(cell.value);
                storage.flush().unwrap();
                socket.send(AnyMessage::Just32(storage.flush_pos()));
            }
        }
    }

    /// Client copies all data from array to the remote node.
    pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
        info!("started client");

        let mut delivered = 0;

        let mut sock = os.open_tcp(dst);
        let mut recv_chan = sock.recv_chan();

        while delivered < data.len() {
            let num = &data[delivered];
            info!("sending data: {:?}", num.clone());
            sock.send(AnyMessage::ReplCell(num.clone()));

            // loop {
            let event = recv_chan.recv();
            match event {
                NetEvent::Message(AnyMessage::Just32(flush_pos)) => {
                    if flush_pos == 1 + delivered as u32 {
                        delivered += 1;
                    }
                }
                NetEvent::Closed => {
                    info!("connection closed, reestablishing");
                    sock = os.open_tcp(dst);
                    recv_chan = sock.recv_chan();
                }
                _ => {}
            }

            // }
        }

        let sock = os.open_tcp(dst);
        for num in data {
            info!("sending data: {:?}", num.clone());
            sock.send(AnyMessage::ReplCell(num.clone()));
        }

        info!("sent all data and finished client");
    }

    /// Run test simulations.
    #[test]
    fn sim_example_reliable_copy() {
        utils::logging::init(
            utils::logging::LogFormat::Test,
            utils::logging::TracingErrorLayerEnablement::Disabled,
            utils::logging::OtelEnablement::Disabled,
            utils::logging::Output::Stdout,
        )
        .expect("logging init failed");

        let delay = Delay {
            min: 1,
            max: 60,
            fail_prob: 0.4,
        };

        let network = NetworkOptions {
            keepalive_timeout: Some(50),
            connect_delay: delay.clone(),
            send_delay: delay.clone(),
        };

        for seed in 0..20 {
            let u32_data: [u32; 5] = [1, 2, 3, 4, 5];
            let data = u32_to_cells(&u32_data, 1);
            let world = Arc::new(World::new(seed, Arc::new(network.clone())));

            start_simulation(Options {
                world,
                time_limit: 1_000_000,
                client_fn: Box::new(move |os, server_id| run_client(os, &data, server_id)),
                u32_data,
            });
        }
    }

    pub struct Options {
        pub world: Arc<World>,
        pub time_limit: u64,
        pub u32_data: [u32; 5],
        pub client_fn: Box<dyn FnOnce(NodeOs, u32) + Send + 'static>,
    }

    pub fn start_simulation(options: Options) {
        let world = options.world;

        let client_node = world.new_node();
        let server_node = world.new_node();
        let server_id = server_node.id;

        // start the client thread
        client_node.launch(move |os| {
            let client_fn = options.client_fn;
            client_fn(os, server_id);
        });

        // start the server thread
        let shared_storage = SharedStorage::new();
        let server_storage = shared_storage.clone();
        server_node.launch(move |os| run_server(os, Box::new(server_storage)));

        while world.step() && world.now() < options.time_limit {}

        let disk_data = shared_storage.state.lock().data.clone();
        assert!(verify_data(&disk_data, &options.u32_data[..]));
    }

    pub fn u32_to_cells(data: &[u32], client_id: u32) -> Vec<ReplCell> {
        let mut res = Vec::new();
        for (i, _) in data.iter().enumerate() {
            res.push(ReplCell {
                client_id,
                seqno: i as u32,
                value: data[i],
            });
        }
        res
    }

    fn verify_data(disk_data: &[u32], data: &[u32]) -> bool {
        if disk_data.len() != data.len() {
            return false;
        }
        for i in 0..data.len() {
            if disk_data[i] != data[i] {
                return false;
            }
        }
        true
    }
}
