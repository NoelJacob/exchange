use crate::types::{ExecutionMessage, ExpectedFill};

#[derive(Debug, Clone)]
pub struct FillValidator {
    pub expected_fills: Vec<ExpectedFill>,
    pub pass_count: u64,
    pub fail_count: u64,
}

impl FillValidator {
    pub fn new() -> Self {
        Self {
            expected_fills: Vec::new(),
            pass_count: 0,
            fail_count: 0,
        }
    }

    /// Queues expected fills for future validation.
    pub fn register_expected(&mut self, expected: Vec<ExpectedFill>) {
        self.expected_fills.extend(expected);
    }

    /// Validates an execution against the expected fills queue.
    /// Matches by resting_order_id against exec.order_id.
    /// Returns true if a matching expected fill exists and price/qty match within tolerance.
    pub fn validate_execution(&mut self, exec: &ExecutionMessage) -> bool {
        let pos = self
            .expected_fills
            .iter()
            .position(|ef| ef.resting_order_id == exec.order_id);

        match pos {
            Some(idx) => {
                let expected = self.expected_fills.remove(idx);

                let price_ok = (expected.fill_price - exec.fill_price).abs() < 0.001;
                let qty_ok = expected.fill_qty == exec.fill_qty;

                if price_ok && qty_ok {
                    self.pass_count += 1;
                    true
                } else {
                    self.fail_count += 1;
                    false
                }
            }
            None => {
                self.fail_count += 1;
                false
            }
        }
    }

    /// Returns pass / (pass + fail), or 0 if no fills recorded.
    pub fn correctness(&self) -> f64 {
        let total = self.pass_count + self.fail_count;
        if total == 0 {
            0.0
        } else {
            self.pass_count as f64 / total as f64
        }
    }
}
