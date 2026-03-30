mod macros;

yazi_macro::mod_pub!(fetch file hook plugin preload process size);

yazi_macro::mod_flat!(ongoing op out progress runner scheduler snap task transfer_log);

pub use runner::{PausedTransferJob, TransferJobKind};

const LOW: u8 = yazi_config::Priority::Low as u8;
const NORMAL: u8 = yazi_config::Priority::Normal as u8;
const HIGH: u8 = yazi_config::Priority::High as u8;
