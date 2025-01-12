use crate::client::SocketConfig;
use crate::config::Host;
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error, RawConnection};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub async fn connect<T>(
    mut tls: T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    let hostname = match &config.host {
        Host::Tcp(host) => host.as_str(),
    };

    let tls = tls
        .make_tls_connect(hostname)
        .map_err(|e| Error::tls(e.into()))?;

    match connect_once(&config.host, config.port, tls, config).await {
        Ok((client, connection)) => Ok((client, connection)),
        Err(e) => Err(e),
    }
}

async fn connect_once<T>(
    host: &Host,
    port: u16,
    tls: T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: TlsConnect<TcpStream>,
{
    let socket = connect_socket(host, port, config.connect_timeout).await?;
    let RawConnection {
        stream,
        parameters,
        delayed_notice: _,
        process_id,
        secret_key,
    } = connect_raw(socket, tls, config).await?;

    let socket_config = SocketConfig {
        host: host.clone(),
        port,
        connect_timeout: config.connect_timeout,
    };

    let (sender, receiver) = mpsc::unbounded_channel();
    let client = Client::new(
        sender,
        socket_config,
        config.ssl_mode,
        process_id,
        secret_key,
    );

    let connection = Connection::new(stream, parameters, receiver);

    Ok((client, connection))
}
