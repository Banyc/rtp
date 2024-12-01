use core::time::Duration;

use primitive::ops::float::NonNegR;

/// ref: <https://datatracker.ietf.org/doc/html/rfc6298>
#[derive(Debug, Clone)]
pub struct RtxTimer {
    smooth_rtt: NonNegR<f64>,
    smooth_rtt_var: NonNegR<f64>,
    first_measured: bool,
}
impl RtxTimer {
    const MIN_RTO: Duration = Duration::from_secs(1);
    const K: f64 = 4.;
    const BETA: f64 = 1. / 4.;
    const ALPHA: f64 = 1. / 8.;

    pub fn new() -> Self {
        Self {
            smooth_rtt: NonNegR::new(Self::MIN_RTO.as_secs_f64()).unwrap(),
            smooth_rtt_var: NonNegR::new(0.0).unwrap(),
            first_measured: false,
        }
    }

    pub fn set(&mut self, rtt: Duration) {
        if !self.first_measured {
            self.first_measured = true;
            self.smooth_rtt = NonNegR::new(rtt.as_secs_f64()).unwrap();
            self.smooth_rtt_var = NonNegR::new(rtt.as_secs_f64() / 2.).unwrap();
            return;
        }

        let rtt_var = (self.smooth_rtt.get() - rtt.as_secs_f64()).abs();
        let smooth_rtt_var = (1. - Self::BETA) * self.smooth_rtt_var.get() + Self::BETA * rtt_var;
        self.smooth_rtt_var = NonNegR::new(smooth_rtt_var).unwrap();

        let smooth_rtt =
            (1. - Self::ALPHA) * self.smooth_rtt.get() + Self::ALPHA * rtt.as_secs_f64();
        self.smooth_rtt = NonNegR::new(smooth_rtt).unwrap();
    }

    // pub fn rto(&self, granularity: Duration) -> Duration {
    //     let tol = Self::K * self.smooth_rtt_var.get();
    //     let rto = self.smooth_rtt.get() + granularity.as_secs_f64().max(tol);
    //     Duration::from_secs_f64(rto).max(Self::MIN_RTO)
    // }
    pub fn rto(&self) -> Duration {
        let tol = Self::K * self.smooth_rtt_var.get();
        let rto = self.smooth_rtt.get() + tol;
        Duration::from_secs_f64(rto).max(Self::MIN_RTO)
    }

    pub fn smooth_rtt(&self) -> Duration {
        Duration::from_secs_f64(self.smooth_rtt.get())
    }
}
