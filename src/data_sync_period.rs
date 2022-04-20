pub enum DataSyncronizationPeriod {
    Immediately,
    Sec1,
    Sec5,
    Sec15,
    Sec30,
    Min1,
    Asap,
}

impl DataSyncronizationPeriod {
    pub fn as_html_param_value(&self) -> &str {
        match self {
            DataSyncronizationPeriod::Immediately => "i",
            DataSyncronizationPeriod::Sec1 => "1",
            DataSyncronizationPeriod::Sec5 => "5",
            DataSyncronizationPeriod::Sec15 => "15",
            DataSyncronizationPeriod::Sec30 => "30",
            DataSyncronizationPeriod::Min1 => "50",
            DataSyncronizationPeriod::Asap => "a",
        }
    }
}
