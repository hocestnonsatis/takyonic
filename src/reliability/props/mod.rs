//! Property-based tests (proptest) for MVCC/OCC, 2PC, SIMD/JIT equivalence.

#[cfg(test)]
mod mvcc_occ;
#[cfg(test)]
mod twopc;
#[cfg(test)]
mod simd_jit;

#[cfg(test)]
mod tests {
    #[test]
    fn props_module_loads() {
        assert_eq!(2 + 2, 4);
    }
}
