//! Error struct and methods

use std::io;

use std::fmt;

#[cfg(feature = "security")]
use rustls::TLSError;

use failure::Fail;
use crate::failure::{Context, Backtrace};

/// An error as reported by a remote Kafka server

#[derive(Debug, Fail)]
pub enum KafkaErrorKind {
#[cfg(feature = "security")]
// The various errors this library can produce.
    #[fail(display = "KafkaError: {:?}", _0)]
     Kafka(KafkaErrorCode),

    /// An error when transmitting a request for a particular topic and partition.
    /// Contains the topic and partition of the request that failed,
    /// and the error code as reported by the Kafka server, respectively.
    #[fail(display = "Topic Partition Error ({:?}, {:?}, {:?})", topic_name, partition_id, error_code)]
     TopicPartitionError {
        //description("Error in request for topic and partition")
        topic_name: String,
        partition_id: i32,
        error_code: KafkaErrorCode
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
    TLSError(#[fail(cause)] TLSError),
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
#[derive(Fail, Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaErrorCode {
    #[fail(display = "An unexpected server error")]
    Unknown = -1,

    /// maintained by the server for the given topic/partition
    #[fail(display = "The requested offset is outside the range of offsets")]
    OffsetOutOfRange = 1,

    #[fail(display = "This indicates that a message contents does not match its CRC")]
    CorruptMessage = 2,

    #[fail(display = "This request is for a topic or partition is not known")]
    UnknownTopicOrPartition = 3,

    #[fail(display = "The message has a negative size")]
    InvalidMessageSize = 4,

    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    #[fail(display = "The Leader is not currently available")]
    LeaderNotAvailable = 5,

    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    #[fail(display = "This error is thrown if the client attempts to send messages")]
    NotLeaderForPartition = 6,

    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    #[fail(display = "RequestTimedOut")]
    RequestTimedOut = 7,

    /// when a broker is not alive.
    #[fail(display = "BrokerNotAvailable")]
    BrokerNotAvailable = 8,

    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    #[fail(display = "ReplicaNotAvailable")]
    ReplicaNotAvailable = 9,

    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    #[fail(display = "The server has a configurable maximum message size to avoid")]
    MessageSizeTooLarge = 10,

    /// Internal error code for broker-to-broker communication.
    #[fail(display = "StaleControllerEpoch")]
    StaleControllerEpoch = 11,

    /// offset metadata
    #[fail(display = "Offset Metadata Too Large")]
    OffsetMetadataTooLarge = 12,

    /// The server disconnected before a response was received.
    #[fail(display = "Network Exception")]
    NetworkException = 13,

    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    #[fail(display = "Group Load In Progress")]
    GroupLoadInProgress = 14,

    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    #[fail(display = "Group coordinator not available")]
    GroupCoordinatorNotAvailable = 15,

    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    #[fail(display = "Not Coordinator for Group")]
    NotCoordinatorForGroup = 16,

    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    #[fail(display = "Invalid Topic")]
    InvalidTopic = 17,

    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    #[fail(display = "Record List Too Large")]
    RecordListTooLarge = 18,

    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    #[fail(display = "Not Enough Replicas")]
    NotEnoughReplicas = 19,

    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    #[fail(display = "Not Enough Replicas After Append")]
    NotEnoughReplicasAfterAppend = 20,

    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    #[fail(display = "Invliad Required Acks")]
    InvalidRequiredAcks = 21,

    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    #[fail(display = "Illegal Generation")]
    IllegalGeneration = 22,

    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    #[fail(display = "Inconsistent Group Protocol")]
    InconsistentGroupProtocol = 23,

    /// Returned in join group when the groupId is empty or null.
    #[fail(display = "Invalid Group Id")]
    InvalidGroupId = 24,

    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    #[fail(display = "Unknown Member Id")]
    UnknownMemberId = 25,

    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    #[fail(display = "Invalid Session Timeout")]
    InvalidSessionTimeout = 26,

    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    #[fail(display = "Rebalance in Progress")]
    RebalanceInProgress = 27,

    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    #[fail(display = "Invalid Commit Offset Size")]
    InvalidCommitOffsetSize = 28,

    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    #[fail(display = "Topic Authorization Failed")]
    TopicAuthorizationFailed = 29,

    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    #[fail(display = "Group Authorization Failed")]
    GroupAuthorizationFailed = 30,

    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    #[fail(display = "Cluster Authorization failed")]
    ClusterAuthorizationFailed = 31,

    /// The timestamp of the message is out of acceptable range.
    #[fail(display = "Invalid Timestamp")]
    InvalidTimestamp = 32,

    /// The broker does not support the requested SASL mechanism.
    #[fail(display = "Unsupported Sasl Mechanism")]
    UnsupportedSaslMechanism = 33,

    /// Request is not valid given the current SASL state.
    #[fail(display = "Illegal Sasl State")]
    IllegalSaslState = 34,

    /// The version of API is not supported.
    #[fail(display = "Unsupported Version")]
    UnsupportedVersion = 35,
}


#[cfg(feature = "security")]
impl From<&TLSError> for KafkaErrorKind {
    fn from(err: &TLSError) -> KafkaErrorKind {
        KafkaErrorKind::TLSError(err.clone())
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


//impl KafkaError {
//    pub fn kind(&self) -> KafkaErrorKind {
//        *self.inner.get_context()
//    }
//}

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
