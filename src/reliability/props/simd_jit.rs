//! SIMD and JIT outputs must match scalar interpreters on random inputs.

use proptest::prelude::*;

use crate::jit::JitCompiler;
use crate::sql::ArithOp;
use crate::vector::{euclidean_f32, euclidean_simd};
use crate::vectorized::SimdKernels;

fn scalar_mul(a: &[f64], b: &[f64], n: usize) -> Vec<f64> {
    (0..n).map(|i| a[i] * b[i]).collect()
}

fn scalar_sum(vals: &[f64], n: usize) -> f64 {
    vals[..n].iter().copied().sum()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn prop_simd_mul_matches_scalar(
        a in prop::collection::vec(-1.0e6f64..1.0e6, 0..257),
        b in prop::collection::vec(-1.0e6f64..1.0e6, 0..257),
    ) {
        let n = a.len().min(b.len());
        let mut out = vec![0.0; n];
        SimdKernels::mul(&a, &b, &mut out, n);
        let expect = scalar_mul(&a, &b, n);
        for i in 0..n {
            prop_assert_eq!(out[i], expect[i], "lane {}", i);
        }
    }

    #[test]
    fn prop_simd_sum_matches_scalar(
        vals in prop::collection::vec(-1.0e6f64..1.0e6, 0..513),
    ) {
        let n = vals.len();
        let got = SimdKernels::sum(&vals, n);
        let expect = scalar_sum(&vals, n);
        let ok = got == expect
            || (got - expect).abs() <= 1e-9 * expect.abs().max(1.0);
        prop_assert!(ok, "sum got={got} expect={expect} n={n}");
    }

    #[test]
    fn prop_euclidean_simd_matches_scalar(
        dims in 1usize..65,
        seed in any::<u32>(),
    ) {
        let mut a = vec![0f32; dims];
        let mut b = vec![0f32; dims];
        let mut s = seed;
        for i in 0..dims {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            a[i] = ((s % 2000) as f32) * 0.01 - 10.0;
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            b[i] = ((s % 2000) as f32) * 0.01 - 10.0;
        }
        let got = euclidean_simd(&a, &b);
        let expect = euclidean_f32(&a, &b);
        prop_assert!(
            (got - expect).abs() <= 1e-4 * expect.abs().max(1.0),
            "euclid got={got} expect={expect} dims={dims}"
        );
    }

    #[test]
    fn prop_jit_batch_mul_matches_scalar(
        raw_a in prop::collection::vec(-1000.0f64..1000.0, 8..129),
        raw_b in prop::collection::vec(-1000.0f64..1000.0, 8..129),
    ) {
        let n = raw_a.len().min(raw_b.len());
        let a = &raw_a[..n];
        let b = &raw_b[..n];
        let jit = JitCompiler::new().expect("jit");
        let kernel = jit.compile_batch_arith(ArithOp::Mul).expect("compile");
        let mut out = vec![0.0; n];
        // SAFETY: a/b/out length >= n; kernel from finalized JIT module.
        unsafe {
            kernel(a.as_ptr(), b.as_ptr(), out.as_mut_ptr(), n as i64);
        }
        for i in 0..n {
            let expect = a[i] * b[i];
            prop_assert!(
                (out[i] - expect).abs() < 1e-9,
                "jit lane {i}: {} vs {expect}",
                out[i]
            );
        }
    }
}
