//! The core: the module, its instances, and typed wrappers over its machines (I-23, I-25).

mod machine;
mod module;

pub use machine::{AgentMachine, PlanMachine, RunMachine};
pub use module::Core;
