//! Bounded browser workloads and coordinator-side numerical/result checks.
use serde::{Deserialize, Serialize};

pub const MODEL: &str = "minilm-l6-v2";
pub const MODEL_REPOSITORY: &str = "Xenova/all-MiniLM-L6-v2";
pub const MODEL_REVISION: &str = "751bff37182d3f1213fa05d7196b954e230abad9";
pub const EMBEDDING_DIMENSIONS: usize = 384;
pub const TASK_TIMEOUT_MS: u64 = 300_000;

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InferenceVerification {
    #[default]
    Replicated,
    Single,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    VectorAdd {
        a: Vec<f32>,
        b: Vec<f32>,
    },
    Matmul {
        a: Vec<f32>,
        b: Vec<f32>,
        rows: usize,
        inner: usize,
        cols: usize,
    },
    Embedding {
        model: String,
        texts: Vec<String>,
        #[serde(default)]
        verification: InferenceVerification,
    },
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub values: Vec<f32>,
    pub shape: Vec<usize>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeReport {
    pub backend: String,
    pub compute_ms: f64,
    pub load_ms: f64,
    pub fallback_reason: Option<String>,
}
impl RuntimeReport {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !matches!(self.backend.as_str(), "webgpu" | "wasm" | "cpu")
            || [self.compute_ms, self.load_ms]
                .iter()
                .any(|ms| !ms.is_finite() || *ms < 0.0 || *ms > TASK_TIMEOUT_MS as f64)
            || self
                .fallback_reason
                .as_ref()
                .is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
        {
            return Err("Invalid worker runtime report");
        }
        Ok(())
    }
}
fn tensor(values: &[f32]) -> bool {
    !values.is_empty()
        && values.len() <= 8192
        && values
            .iter()
            .all(|v| v.is_finite() && v.abs() <= 1_000_000.0)
}
impl Input {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::VectorAdd { a, b }
                if tensor(a) && tensor(b) && a.len() == b.len() && a.len() <= 4096 =>
            {
                Ok(())
            }
            Self::Matmul {
                a,
                b,
                rows,
                inner,
                cols,
            } if (1..=128).contains(rows)
                && (1..=128).contains(inner)
                && (1..=128).contains(cols)
                && tensor(a)
                && tensor(b)
                && a.len() == rows * inner
                && b.len() == inner * cols
                && a.len() + b.len() <= 8192
                && rows * cols <= 4096
                && rows * inner * cols <= 262_144 =>
            {
                Ok(())
            }
            Self::Embedding { model, texts, .. }
                if model == MODEL
                    && !texts.is_empty()
                    && texts.len() <= 4
                    && texts
                        .iter()
                        .all(|s| !s.trim().is_empty() && s.len() <= 2048)
                    && texts.iter().map(String::len).sum::<usize>() <= 8192 =>
            {
                Ok(())
            }
            _ => Err("Unsupported operation/model or input exceeds the browser workload limits"),
        }
    }
    pub fn replicas(&self) -> usize {
        if matches!(
            self,
            Self::Embedding {
                verification: InferenceVerification::Replicated,
                ..
            }
        ) {
            2
        } else {
            1
        }
    }
    pub fn shape(&self) -> Vec<usize> {
        match self {
            Self::VectorAdd { a, .. } => vec![a.len()],
            Self::Matmul { rows, cols, .. } => vec![*rows, *cols],
            Self::Embedding { texts, .. } => vec![texts.len(), EMBEDDING_DIMENSIONS],
        }
    }
    pub fn verification(&self) -> &'static str {
        match self {
            Self::Embedding {
                verification: InferenceVerification::Single,
                ..
            } => "single_worker_unverified",
            Self::Embedding { .. } => "two_node_agreement",
            _ => "server_numeric",
        }
    }
    pub fn validate_output(&self, output: &Output) -> Result<(), &'static str> {
        self.validate()?;
        if output.shape != self.shape()
            || output.values.len() != output.shape.iter().product::<usize>()
            || output.values.iter().any(|v| !v.is_finite())
        {
            return Err("Worker result shape or values are invalid");
        }
        match self {
            Self::VectorAdd { a, b } => {
                if a.iter().zip(b).zip(&output.values).any(|((a, b), actual)| {
                    let expected = (*a + *b) as f64;
                    (*actual as f64 - expected).abs() > 1e-5 + 1e-5 * expected.abs()
                }) {
                    return Err("Vector result failed the full coordinator comparison");
                }
            }
            Self::Matmul {
                a,
                b,
                rows,
                inner,
                cols,
            } => {
                for row in 0..*rows {
                    for col in 0..*cols {
                        let mut expected = 0.0f64;
                        let mut magnitude = 0.0f64;
                        for k in 0..*inner {
                            let value = a[row * inner + k] as f64 * b[k * cols + col] as f64;
                            expected += value;
                            magnitude += value.abs();
                        }
                        if (output.values[row * cols + col] as f64 - expected).abs()
                            > 1e-4 + 2e-5 * magnitude
                        {
                            return Err("Matrix result failed the full coordinator comparison");
                        }
                    }
                }
            }
            Self::Embedding { .. } => {
                for vector in output.values.as_chunks::<EMBEDDING_DIMENSIONS>().0 {
                    let norm = vector
                        .iter()
                        .map(|v| (*v as f64).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    if !(0.95..=1.05).contains(&norm) {
                        return Err("Embedding vectors must be finite and normalized");
                    }
                }
            }
        }
        Ok(())
    }
    pub fn agrees(&self, left: &Output, right: &Output) -> bool {
        if self.validate_output(left).is_err() || self.validate_output(right).is_err() {
            return false;
        }
        if matches!(self, Self::Embedding { .. }) {
            left.values
                .as_chunks::<EMBEDDING_DIMENSIONS>()
                .0
                .iter()
                .zip(right.values.as_chunks::<EMBEDDING_DIMENSIONS>().0.iter())
                .all(|(a, b)| {
                    let dot = a
                        .iter()
                        .zip(b)
                        .map(|(a, b)| *a as f64 * *b as f64)
                        .sum::<f64>();
                    let na = a.iter().map(|v| (*v as f64).powi(2)).sum::<f64>().sqrt();
                    let nb = b.iter().map(|v| (*v as f64).powi(2)).sum::<f64>().sqrt();
                    dot / (na * nb) >= 0.9995 && a.iter().zip(b).all(|(a, b)| (a - b).abs() <= 0.01)
                })
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_inputs_and_complete_numerical_validation() {
        let job = Input::Matmul {
            a: vec![1., 2., 3., 4.],
            b: vec![5., 6., 7., 8.],
            rows: 2,
            inner: 2,
            cols: 2,
        };
        assert!(job.validate().is_ok());
        assert!(job
            .validate_output(&Output {
                values: vec![19., 22., 43., 50.],
                shape: vec![2, 2]
            })
            .is_ok());
        assert!(job
            .validate_output(&Output {
                values: vec![19., 22., 43., 51.],
                shape: vec![2, 2]
            })
            .is_err());
        assert!(Input::VectorAdd {
            a: vec![f32::NAN],
            b: vec![1.]
        }
        .validate()
        .is_err());
        assert!(Input::Matmul {
            a: vec![0.; 8192],
            b: vec![0.; 8192],
            rows: 128,
            inner: 128,
            cols: 128
        }
        .validate()
        .is_err());
        assert!(Input::Embedding {
            model: "arbitrary-code".into(),
            texts: vec!["hello".into()],
            verification: InferenceVerification::Single
        }
        .validate()
        .is_err());
    }
    #[test]
    fn embedding_shape_is_not_semantic_verification_and_replicas_must_agree() {
        let input = Input::Embedding {
            model: MODEL.into(),
            texts: vec!["hello".into()],
            verification: InferenceVerification::Replicated,
        };
        assert_eq!(input.replicas(), 2);
        let mut a = Output {
            values: vec![0.; 384],
            shape: vec![1, 384],
        };
        a.values[0] = 1.;
        let mut b = Output {
            values: vec![0.; 384],
            shape: vec![1, 384],
        };
        b.values[1] = 1.;
        assert!(input.validate_output(&a).is_ok());
        assert!(input.validate_output(&b).is_ok());
        assert!(input.agrees(&a, &a));
        assert!(!input.agrees(&a, &b));
        assert!(input
            .validate_output(&Output {
                values: vec![0.; 384],
                shape: vec![1, 384]
            })
            .is_err());
    }
}
