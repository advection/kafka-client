//! Error struct and methods

use std::io;

use std::fmt;

#[cfg(feature = "security")]
use openssl::error::ErrorStack;
#[cfg(feature = "security")]
use openssl::ssl::{self, Error as SslError};
#[cfg(feature = "security")]

use failure::Fail;
use crate::failure::{Context, Backtrace};

/// An error as reported by a remote Kafka server

#[derive(Debug, Fail)]
pub enum KafkaErrorKind {

    #[fail(display = "KafkaError: {:?}", _0)]
     Kafka(KafkaCode),

    /// An error when transmitting a request for a particular topic and partition.
    /// Contains the topic and partition of the request that failed,
    /// and the error code as reported by the Kafka server, respectively.
    #[fail(display = "Topic Partition Error ({:?}, {:?}, {:?})", topic_name, partition_id, error_code)]
     TopicPartitionError {
        //description("Error in request for topic and partition")
        topic_name: String,
        partition_id: i32,
        error_code: KafkaCode
    },

    /// Failure to correctly parse the server response due to the
    /// server speaking a newer protocol version (than the one this
    /// library supports)
    #[fail(display = "Unsupported protocol version")]
    UnsupportedProtocol,

    /// Failure to correctly parse the server response by this library
    /// due to an unsupported compression format of the data
    #[fail(display = "Unsupported compression format")]
    UnsupportedCompression,

    /// Failure to decode or encode a response or request respectively
    #[fail(display = "Encoding/Decoding Error")]
    CodecError,

    /// Failure to decode a string into a valid utf8 byte sequence
    #[fail(display = "String decoding error")]
    StringDecodeError,

    /// Unable to reach any host
    #[fail(display = "No host reachable")]
    NoHostReachable,

    /// Unable to set up `Consumer` due to missing topic assignments
    #[fail(display = "No topic assigned")]
    NoTopicsAssigned,

    /// An invalid user-provided duration
    #[fail(display = "Invalid duration")]
    InvalidDuration,

    #[cfg(feature = "snappy")]
//    #[doc="Failure to encode/decode a snappy compressed response from Kafka"]
    #[fail(display = "{}", _0)]
    InvalidSnappy(#[fail(cause)] ::snap::Error),

    #[fail(display = "{}", _0)]
    IoError(#[fail(cause)] io::Error),

    #[cfg(feature = "security")]
    #[fail(display = "{}", _0)]
    SSLError(#[fail(cause)] SslError),

    #[cfg(feature = "security")]
    #[fail(display = "{}", _0)]
    SSLHandshakeError(#[fail(cause)] ErrorStack),
}


#[cfg(feature = "snappy")]
pub fn from_snap_error_ref(err: &::snap::Error) -> KafkaErrorKind {
    match err {
        &::snap::Error::TooBig { given, max } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::TooBig { given, max })
        }
        &::snap::Error::BufferTooSmall { given, min } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::BufferTooSmall { given, min })
        }
        ::snap::Error::Empty => KafkaErrorKind::InvalidSnappy(::snap::Error::Empty),
        ::snap::Error::Header => KafkaErrorKind::InvalidSnappy(::snap::Error::Header),
        &::snap::Error::HeaderMismatch {
            expected_len,
            got_len,
        } => KafkaErrorKind::InvalidSnappy(::snap::Error::HeaderMismatch {
            expected_len,
            got_len,
        }),
        &::snap::Error::Literal {
            len,
            src_len,
            dst_len,
        } => KafkaErrorKind::InvalidSnappy(::snap::Error::Literal {
            len,
            src_len,
            dst_len,
        }),
        &::snap::Error::CopyRead { len, src_len } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::CopyRead { len, src_len })
        }
        &::snap::Error::CopyWrite { len, dst_len } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::CopyWrite { len, dst_len })
        }
        &::snap::Error::Offset { offset, dst_pos } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::Offset { offset, dst_pos })
        }
        &::snap::Error::StreamHeader { byte } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::StreamHeader { byte })
        }
        ::snap::Error::StreamHeaderMismatch { ref bytes } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::StreamHeaderMismatch {
                bytes: bytes.clone(),
            })
        }
        &::snap::Error::UnsupportedChunkType { byte } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::UnsupportedChunkType { byte })
        }
        &::snap::Error::UnsupportedChunkLength { len, header } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::UnsupportedChunkLength { len, header })
        }
        &::snap::Error::Checksum { expected, got } => {
            KafkaErrorKind::InvalidSnappy(::snap::Error::Checksum { expected, got })
        }
    }
}

#[derive(Debug)]
struct KafkaError {
    inner: Context<KafkaErrorKind>,
}

// zlb: seems like it might make sense to make these also error enums
/// Various errors reported by a remote Kafka server.
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaCode {
    /// An unexpected server error
    Unknown = -1,
    /// The requested offset is outside the range of offsets
    /// maintained by the server for the given topic/partition
    OffsetOutOfRange = 1,
    /// This indicates that a message contents does not match its CRC
    CorruptMessage = 2,
    /// This request is for a topic or partition that does not exist
    /// on this broker.
    UnknownTopicOrPartition = 3,
    /// The message has a negative size
    InvalidMessageSize = 4,
    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    LeaderNotAvailable = 5,
    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    NotLeaderForPartition = 6,
    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    RequestTimedOut = 7,
    /// This is not a client facing error and is used mostly by tools
    /// when a broker is not alive.
    BrokerNotAvailable = 8,
    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    ReplicaNotAvailable = 9,
    /// The server has a configurable maximum message size to avoid
    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    MessageSizeTooLarge = 10,
    /// Internal error code for broker-to-broker communication.
    StaleControllerEpoch = 11,
    /// If you specify a string larger than configured maximum for
    /// offset metadata
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    GroupLoadInProgress = 14,
    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    GroupCoordinatorNotAvailable = 15,
    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    NotCoordinatorForGroup = 16,
    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    InvalidTopic = 17,
    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    RecordListTooLarge = 18,
    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    NotEnoughReplicas = 19,
    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    InvalidRequiredAcks = 21,
    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    IllegalGeneration = 22,
    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    InconsistentGroupProtocol = 23,
    /// Returned in join group when the groupId is empty or null.
    InvalidGroupId = 24,
    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    UnknownMemberId = 25,
    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    InvalidSessionTimeout = 26,
    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    RebalanceInProgress = 27,
    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    InvalidCommitOffsetSize = 28,
    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    TopicAuthorizationFailed = 29,
    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    GroupAuthorizationFailed = 30,
    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
}

/*#[cfg(feature = "security")]
impl<S> From<ssl::HandshakeError<S>> for Error {
    fn from(err: ssl::HandshakeError<S>) -> Error {
        match err {
            ssl::HandshakeError::SetupFailure(e) => From::from(e),
            ssl::HandshakeError::Failure(s) | ssl::HandshakeError::Interrupted(s) => {
                from_sslerror_ref(s.error()).into()
            }
        }
    }
}
*/

#[cfg(feature = "security")]
fn from_sslerror_ref(err: &ssl::Error) -> KafkaErrorKind {
    match err {
        SslError::ZeroReturn => KafkaErrorKind::SSLError(SslError::ZeroReturn),
        SslError::WantRead(ref e) => KafkaErrorKind::SSLError(SslError::WantRead(clone_ioe(e))),
        SslError::WantWrite(ref e) => KafkaErrorKind::SSLError(SslError::WantWrite(clone_ioe(e))),
        SslError::WantX509Lookup => KafkaErrorKind::SSLError(SslError::WantX509Lookup),
        SslError::Stream(ref e) => KafkaErrorKind::SSLError(SslError::Stream(clone_ioe(e))),
        SslError::Ssl(ref es) => KafkaErrorKind::SSLError(SslError::Ssl(es.clone())),
    }
}



/// Attempt to clone `io::Error`.
fn clone_ioe(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), format!("Io error: {}", e)),
    }
}

impl Fail for KafkaError {
    fn cause(&self) -> Option<& dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}


impl KafkaError {
    pub fn kind(&self) -> KafkaErrorKind {
        *self.inner.get_context()
    }
}

impl From<KafkaErrorKind> for KafkaError {
    fn from(kind: KafkaErrorKind) -> KafkaError {
        KafkaError { inner: Context::new(kind) }
    }
}

impl From<Context<KafkaErrorKind>> for KafkaError {
    fn from(inner: Context<KafkaErrorKind>) -> KafkaError {
        KafkaError { inner: inner }
    }
}



/*impl Clone for Error {
    fn clone(&self) -> Error {
        match self {
            Error(KafkaErrorKind::Io(err)) => KafkaErrorKind::Io(clone_ioe(err)).into(),
            &Error(KafkaErrorKind::Kafka(x)) => KafkaErrorKind::Kafka(x).into(),
            &Error(KafkaErrorKind::TopicPartitionError(ref topic, partition, error_code)) => {
                KafkaErrorKind::TopicPartitionError(topic.clone(), partition, error_code).into()
            }
            #[cfg(feature = "security")]
            Error(KafkaErrorKind::Ssl(ref x)) => from_sslerror_ref(x).into(),
            #[cfg(feature = "security")]
            Error(KafkaErrorKind::SslHandshake(ref x)) => KafkaErrorKind::SslHandshake(x.clone()).into(),
            Error(KafkaErrorKind::UnsupportedProtocol) => KafkaErrorKind::UnsupportedProtocol.into(),
            Error(KafkaErrorKind::UnsupportedCompression) => KafkaErrorKind::UnsupportedCompression.into(),
            #[cfg(feature = "snappy")]
            Error(KafkaErrorKind::InvalidSnappy(ref err)) => from_snap_error_ref(err).into(),
            Error(KafkaErrorKind::UnexpectedEOF) => KafkaErrorKind::UnexpectedEOF.into(),
            Error(KafkaErrorKind::CodecError) => KafkaErrorKind::CodecError.into(),
            Error(KafkaErrorKind::StringDecodeError) => KafkaErrorKind::StringDecodeError.into(),
            Error(KafkaErrorKind::NoHostReachable) => KafkaErrorKind::NoHostReachable.into(),
            Error(KafkaErrorKind::NoTopicsAssigned) => KafkaErrorKind::NoTopicsAssigned.into(),
            Error(KafkaErrorKind::InvalidDuration) => KafkaErrorKind::InvalidDuration.into(),
            Error(KafkaErrorKind::Msg(ref msg)) => KafkaErrorKind::Msg(msg.clone()).into(),
            Error(k) => KafkaErrorKind::Msg(k.to_string()).into(), // XXX: Strange to have to add this, what is missing?
        }
    }
}*/
