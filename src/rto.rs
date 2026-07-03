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

    /// Reordering window used by the fast-retransmit path.
    ///
    /// RACK-style: `srtt + max(K * rttvar, srtt / 4)`, capped at the full RTO.
    /// No `MIN_RTO` floor so that on stable low-RTT links the window stays tight.
    pub fn reorder_window(&self) -> Duration {
        let srtt = self.smooth_rtt();
        let rttvar = Duration::from_secs_f64(self.smooth_rtt_var.get());
        let tol = rttvar.mul_f64(Self::K);
        let quarter = srtt / 4;
        let extra = tol.max(quarter);
        (srtt + extra).min(self.rto())
    }

    pub fn smooth_rtt(&self) -> Duration {
        Duration::from_secs_f64(self.smooth_rtt.get())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RtxTimer;

    #[test]
    fn reorder_window_tracks_variance_without_min_rto_floor() {
        let mut rto = RtxTimer::new();

        // Steady 100 ms samples: variance collapses, so the reorder window is
        // floored by srtt/4. It stays well below MIN_RTO while rto() is still
        // floored at 1 s.
        for _ in 0..20 {
            rto.set(Duration::from_millis(100));
        }
        let rw = rto.reorder_window();
        assert!(rw >= Duration::from_millis(120), "rw={rw:?}");
        assert!(rw < Duration::from_millis(150), "rw={rw:?}");
        assert!(rto.rto() >= Duration::from_secs(1), "rto={:?}", rto.rto());
        assert!(rw < rto.rto(), "rw={rw:?} rto={:?}", rto.rto());

        // Alternating 100 ms / 900 ms samples: variance grows and K*rttvar
        // dominates, pushing the reorder window above 900 ms. It must never
        // exceed the full RTO.
        let mut rto = RtxTimer::new();
        for _ in 0..10 {
            rto.set(Duration::from_millis(100));
            rto.set(Duration::from_millis(900));
        }
        let rw = rto.reorder_window();
        assert!(rw > Duration::from_millis(900), "rw={rw:?}");
        assert!(rw <= rto.rto(), "rw={rw:?} rto={:?}", rto.rto());
    }
}
