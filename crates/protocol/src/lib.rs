//! Protocol buffers and gRPC service definitions for Bicycle.
//!
//! This crate contains the generated code from the protobuf definitions
//! for both the control plane (JobManager <-> Worker) and data plane
//! (inter-operator communication) protocols.

pub mod control {
    include!("generated/bicycle.control.rs");
}

pub mod data {
    include!("generated/bicycle.data.rs");
}

// Re-export commonly used types
pub use control::control_plane_client::ControlPlaneClient;
pub use control::control_plane_server::{ControlPlane, ControlPlaneServer};
pub use data::data_plane_client::DataPlaneClient;
pub use data::data_plane_server::{DataPlane, DataPlaneServer};
