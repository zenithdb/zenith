//! Defines [`RequestContext`].
//!
//! It is a structure that we use throughout the pageserver to propagate
//! high-level context from places that _originate_ activity down to the
//! shared code paths at the heart of the pageserver. It's inspired by
//! Golang's `context.Context`.
//!
//! For example, in `Timeline::get(page_nr, lsn)` we need to answer the following questions:
//! 1. What high-level activity ([`TaskKind`]) needs this page?
//!    We need that information as a categorical dimension for page access
//!    statistics, which we, in turn, need to guide layer eviction policy design.
//! 2. How should we behave if, to produce the page image, we need to
//!    on-demand download a layer file ([`DownloadBehavior`]).
//!
//! [`RequestContext`] satisfies those needs.
//! The current implementation is a small `struct` that is passed through
//! the call chain by reference.
//!
//! ### Future Work
//!
//! However, we do not intend to stop here, since there are other needs that
//! require carrying information from high to low levels of the app.
//!
//! Most importantly, **cancellation signaling** in response to
//! 1. timeouts (page_service max response time) and
//! 2. lifecycle requests (detach tenant, delete timeline).
//!
//! Related to that, there is sometimes a need to ensure that all tokio tasks spawned
//! by the transitive callees of a request have finished. The keyword here
//! is **Structured Concurrency**, and right now, we use `task_mgr` in most places,
//! `TaskHandle` in some places, and careful code review around `FuturesUnordered`
//! or `JoinSet` in other places.
//!
//! We do not yet have a systematic cancellation story in pageserver, and it is
//! pretty clear that [`RequestContext`] will be responsible for that.
//! So, the API already prepares for this role through the
//! [`RequestContext::detached_child`] and [`RequestContext::attached_child`]  methods.
//! See their doc comments for details on how we will use them in the future.
//!
//! It is not clear whether or how we will enforce Structured Concurrency, and
//! what role [`RequestContext`] will play there.
//! So, the API doesn't prepare us for this topic.
//!
//! Other future uses of `RequestContext`:
//! - Communicate compute & IO priorities (user-initiated request vs. background-loop)
//! - Request IDs for distributed tracing
//! - Request/Timeline/Tenant-scoped log levels
//!
//! RequestContext might look quite different once it supports those features.
//! Likely, it will have a shape similar to Golang's `context.Context`.
//!
//! ### Why A Struct Instead Of Method Parameters
//!
//! What's typical about such information is that it needs to be passed down
//! along the call chain from high level to low level, but few of the functions
//! in the middle need to understand it.
//! Further, it is to be expected that we will need to propagate more data
//! in the future (see the earlier section on future work).
//! Hence, for functions in the middle of the call chain, we have the following
//! requirements:
//! 1. It should be easy to forward the context to callees.
//! 2. To propagate more data from high-level to low-level code, the functions in
//!    the middle should not need to be modified.
//!
//! The solution is to have a container structure ([`RequestContext`]) that
//! carries the information. Functions that don't care about what's in it
//! pass it along to callees.
//!
//! ### Why Not Task-Local Variables
//!
//! One could use task-local variables (the equivalent of thread-local variables)
//! to address the immediate needs outlined above.
//! However, we reject task-local variables because:
//! 1. they are implicit, thereby making it harder to trace the data flow in code
//!    reviews and during debugging,
//! 2. they can be mutable, which enables implicit return data flow,
//! 3. they are restrictive in that code which fans out into multiple tasks,
//!    or even threads, needs to carefully propagate the state.
//!
//! In contrast, information flow with [`RequestContext`] is
//! 1. always explicit,
//! 2. strictly uni-directional because RequestContext is immutable,
//! 3. tangible because a [`RequestContext`] is just a value.
//!    When creating child activities, regardless of whether it's a task,
//!    thread, or even an RPC to another service, the value can
//!    be used like any other argument.
//!
//! The solution is that all code paths are infected with precisely one
//! [`RequestContext`] argument. Functions in the middle of the call chain
//! only need to pass it on.

use crate::task_mgr::TaskKind;

// The main structure of this module, see module-level comment.
#[derive(Clone)]
pub struct RequestContext {
    task_kind: TaskKind,
    download_behavior: DownloadBehavior,
    access_stats_behavior: AccessStatsBehavior,
    page_content_kind: PageContentKind,
}

/// The kind of access to the page cache.
#[derive(Clone, Copy, PartialEq, Eq, Debug, enum_map::Enum, strum_macros::IntoStaticStr)]
pub enum PageContentKind {
    Unknown,
    DeltaLayerSummary,
    DeltaLayerBtreeNode,
    DeltaLayerValue,
    ImageLayerSummary,
    ImageLayerBtreeNode,
    ImageLayerValue,
    InMemoryLayer,
}

/// Desired behavior if the operation requires an on-demand download
/// to proceed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DownloadBehavior {
    /// Download the layer file. It can take a while.
    Download,

    /// Download the layer file, but print a warning to the log. This should be used
    /// in code where the layer file is expected to already exist locally.
    Warn,

    /// Return a PageReconstructError::NeedsDownload error
    Error,
}

/// Whether this request should update access times used in LRU eviction
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AccessStatsBehavior {
    /// Update access times: this request's access to data should be taken
    /// as a hint that the accessed layer is likely to be accessed again
    Update,

    /// Do not update access times: this request is accessing the layer
    /// but does not want to indicate that the layer should be retained in cache,
    /// perhaps because the requestor is a compaction routine that will soon cover
    /// this layer with another.
    Skip,
}

pub struct RequestContextBuilder {
    inner: RequestContext,
}

impl RequestContextBuilder {
    /// A new builder with default settings
    pub fn new(task_kind: TaskKind) -> Self {
        Self {
            inner: RequestContext {
                task_kind,
                download_behavior: DownloadBehavior::Download,
                access_stats_behavior: AccessStatsBehavior::Update,
                page_content_kind: PageContentKind::Unknown,
            },
        }
    }

    pub fn from(original: &RequestContext) -> Self {
        Self {
            inner: original.clone(),
        }
    }

    pub fn task_kind(mut self, b: TaskKind) -> Self {
        self.inner.task_kind = b;
        self
    }

    /// Configure the DownloadBehavior of the context: whether to
    /// download missing layers, and/or warn on the download.
    pub fn download_behavior(mut self, b: DownloadBehavior) -> Self {
        self.inner.download_behavior = b;
        self
    }

    /// Configure the AccessStatsBehavior of the context: whether layer
    /// accesses should update the access time of the layer.
    pub(crate) fn access_stats_behavior(mut self, b: AccessStatsBehavior) -> Self {
        self.inner.access_stats_behavior = b;
        self
    }

    pub(crate) fn page_content_kind(mut self, k: PageContentKind) -> Self {
        self.inner.page_content_kind = k;
        self
    }

    pub fn root(self) -> RequestContext {
        self.inner
    }

    pub fn attached_child(self) -> RequestContext {
        self.inner
    }

    pub fn detached_child(self) -> RequestContext {
        self.inner
    }
}

impl RequestContext {
    /// Create a new RequestContext that has no parent.
    ///
    /// The function is called `new` because, once we add children
    /// to it using `detached_child` or `attached_child`, the context
    /// form a tree (not implemented yet since cancellation will be
    /// the first feature that requires a tree).
    ///
    /// # Future: Cancellation
    ///
    /// The only reason why a context like this one can be canceled is
    /// because someone explicitly canceled it.
    /// It has no parent, so it cannot inherit cancellation from there.
    pub fn new(task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        RequestContextBuilder::new(task_kind)
            .download_behavior(download_behavior)
            .root()
    }

    /// Create a detached child context for a task that may outlive `self`.
    ///
    /// Use this when spawning new background activity that should complete
    /// even if the current request is canceled.
    ///
    /// # Future: Cancellation
    ///
    /// Cancellation of `self` will not propagate to the child context returned
    /// by this method.
    ///
    /// # Future: Structured Concurrency
    ///
    /// We could add the Future as a parameter to this function, spawn it as a task,
    /// and pass to the new task the child context as an argument.
    /// That would be an ergonomic improvement.
    ///
    /// We could make new calls to this function fail if `self` is already canceled.
    pub fn detached_child(&self, task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        RequestContextBuilder::from(self)
            .task_kind(task_kind)
            .download_behavior(download_behavior)
            .detached_child()
    }

    /// Create a child of context `self` for a task that shall not outlive `self`.
    ///
    /// Use this when fanning-out work to other async tasks.
    ///
    /// # Future: Cancellation
    ///
    /// Cancelling a context will propagate to its attached children.
    ///
    /// # Future: Structured Concurrency
    ///
    /// We could add the Future as a parameter to this function, spawn it as a task,
    /// and track its `JoinHandle` inside the `RequestContext`.
    ///
    /// We could then provide another method to allow waiting for all child tasks
    /// to finish.
    ///
    /// We could make new calls to this function fail if `self` is already canceled.
    /// Alternatively, we could allow the creation but not spawn the task.
    /// The method to wait for child tasks would return an error, indicating
    /// that the child task was not started because the context was canceled.
    pub fn attached_child(&self) -> Self {
        RequestContextBuilder::from(self).attached_child()
    }

    /// Use this function when you should be creating a child context using
    /// [`attached_child`] or [`detached_child`], but your caller doesn't provide
    /// a context and you are unwilling to change all callers to provide one.
    ///
    /// Before we add cancellation, we should get rid of this method.
    ///
    /// [`attached_child`]: Self::attached_child
    /// [`detached_child`]: Self::detached_child
    pub fn todo_child(task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        Self::new(task_kind, download_behavior)
    }

    pub fn task_kind(&self) -> TaskKind {
        self.task_kind
    }

    pub fn download_behavior(&self) -> DownloadBehavior {
        self.download_behavior
    }

    pub(crate) fn access_stats_behavior(&self) -> AccessStatsBehavior {
        self.access_stats_behavior
    }

    pub(crate) fn page_content_kind(&self) -> PageContentKind {
        self.page_content_kind
    }
}
