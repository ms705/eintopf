use abomonation::Abomonation;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rpc {
    /// Query the given `view` for all records whose key column matches the given value.
    Query {
        /// The view to query
        view: usize,
        /// The key value to use for the given query's free parameter
        key: i64,
    },

    /// Insert a new `article` record.
    ///
    /// `args` gives the column values for the new record.
    InsertArticle {
        /// Article ID.
        aid: i64,
        /// Article title.
        title: String,
    },

    /// Insert a new `vote` record.
    ///
    /// `args` gives the column values for the new record.
    InsertVote {
        /// Article ID.
        aid: i64,
        /// User ID.
        uid: i64,
    },

    /// Flush any buffered responses.
    Flush,
}

impl Abomonation for Rpc {
    #[inline]
    unsafe fn embalm(&mut self) {
        if let Rpc::Query { mut view, mut key } = *self {
            view.embalm();
            key.embalm();
        }
        if let Rpc::InsertArticle {
            ref mut aid,
            ref mut title,
        } = *self
        {
            aid.embalm();
            title.embalm();
        }
        if let Rpc::InsertVote {
            ref mut aid,
            ref mut uid,
        } = *self
        {
            aid.embalm();
            uid.embalm();
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        if let Rpc::Query { ref view, ref key } = *self {
            view.entomb(bytes);
            key.entomb(bytes);
        }
        if let Rpc::InsertArticle { ref aid, ref title } = *self {
            aid.entomb(bytes);
            title.entomb(bytes);
        }
        if let Rpc::InsertVote { ref aid, ref uid } = *self {
            aid.entomb(bytes);
            uid.entomb(bytes);
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Rpc::Query {
                ref mut view,
                ref mut key,
            } => {
                view.exhume(bytes);
                key.exhume(bytes);
                Some(bytes)
            }
            Rpc::InsertArticle {
                ref mut aid,
                ref mut title,
            } => {
                let temp = bytes;
                bytes = if let Some(bytes) = aid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                let temp = bytes;
                bytes = if let Some(bytes) = title.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                Some(bytes)
            }
            Rpc::InsertVote {
                ref mut aid,
                ref mut uid,
            } => {
                let temp = bytes;
                bytes = if let Some(bytes) = aid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                let temp = bytes;
                bytes = if let Some(bytes) = uid.exhume(temp) {
                    bytes
                } else {
                    return None;
                };
                Some(bytes)
            }
            Rpc::Flush => Some(bytes),
        }
    }
}


