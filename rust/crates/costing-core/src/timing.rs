use std::time::Instant;

use crate::model::StageTimings;

pub fn measure<T, E>(
    timings: &mut StageTimings,
    stage: &'static str,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = Instant::now();
    let result = f();
    timings.insert(stage, started.elapsed().as_secs_f64());
    result
}
