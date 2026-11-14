#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub message: String,
}
impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for HttpError {}
pub fn http(status: u16, message: impl Into<String>) -> anyhow::Error {
    HttpError {
        status,
        message: message.into(),
    }
    .into()
}
pub fn require(condition: bool, message: impl Into<String>) -> anyhow::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(http(400, message))
    }
}
