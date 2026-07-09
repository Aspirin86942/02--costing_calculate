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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measure_records_the_closure_elapsed_time() {
        let mut timings = StageTimings::default();

        let value = measure(&mut timings, "stage", || Ok::<_, ()>("done")).unwrap();

        assert_eq!(value, "done");
        let seconds = timings.stages["stage"];
        assert!(seconds.is_finite());
        assert!(seconds >= 0.0);
    }
}
