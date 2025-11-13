#[macro_export]
macro_rules! throw {
    ($co:expr, $code:expr) => {
        match $code {
            Ok(item) => item,
            Err(err) => {
                $co.yield_(Err(err)).await;
                return;
            }
        }
    };
}
