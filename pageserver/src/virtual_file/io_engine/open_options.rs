//! Enum-dispatch to the `OpenOptions` type of the respective [`super::IoEngineKind`];

use std::{os::fd::OwnedFd, path::Path};

use super::IoEngineKind;

#[derive(Debug, Clone)]
pub enum OpenOptions {
    StdFs(std::fs::OpenOptions),
    TokioEpollUring(tokio_epoll_uring::ops::open_at::OpenOptions),
}

impl Default for OpenOptions {
    fn default() -> Self {
        match super::get() {
            IoEngineKind::StdFs => Self::StdFs(std::fs::OpenOptions::new()),
            IoEngineKind::TokioEpollUring => {
                Self::TokioEpollUring(tokio_epoll_uring::ops::open_at::OpenOptions::new())
            }
        }
    }
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        Self::default()
    }

    pub fn read(&mut self, read: bool) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.read(read);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.read(read);
            }
        }
        self
    }

    pub fn write(&mut self, write: bool) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.write(write);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.write(write);
            }
        }
        self
    }

    pub fn create(&mut self, create: bool) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.create(create);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.create(create);
            }
        }
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.create_new(create_new);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.create_new(create_new);
            }
        }
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.truncate(truncate);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.truncate(truncate);
            }
        }
        self
    }

    pub(in crate::virtual_file) async fn open(self, path: &Path) -> std::io::Result<OwnedFd> {
        match self {
            OpenOptions::StdFs(x) => x.open(path).map(|file| file.into()),
            OpenOptions::TokioEpollUring(x) => {
                let system = tokio_epoll_uring::thread_local_system().await;
                system.open(path, &x).await.map_err(|e| match e {
                    tokio_epoll_uring::Error::Op(e) => e,
                    tokio_epoll_uring::Error::System(system) => {
                        std::io::Error::new(std::io::ErrorKind::Other, system)
                    }
                })
            }
        }
    }
}

impl std::os::unix::prelude::OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.mode(mode);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.mode(mode);
            }
        }
        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
        match self {
            OpenOptions::StdFs(x) => {
                let _ = x.custom_flags(flags);
            }
            OpenOptions::TokioEpollUring(x) => {
                let _ = x.custom_flags(flags);
            }
        }
        self
    }
}
