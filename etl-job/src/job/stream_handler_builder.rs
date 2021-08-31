/// This is meant to aid building a stream handler without having to manually implement the
/// StreamHandler trait.  The challenge is to generate the output streams and without adding some
/// macro magic, I am not sure how to do that because each output would handle a different type.

pub struct StreamHandlerBuilder;
