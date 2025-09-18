#[macro_export]
macro_rules! throw {
    ($code:expr) => {
        match $code {
            Ok(item) => item,
            Err(err) => {
                yield Err(err);
                return;
            }
        }
    };
}
