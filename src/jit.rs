//! Cranelift-based JIT query compiler (HyPer-style push pipelines).
//!
//! Compiles scalar predicates and arithmetic expressions into native machine
//! code so analytical `Scan → Filter → Aggregate` segments avoid Volcano
//! virtual-call overhead on every row. Unsupported expressions (strings,
//! subqueries, regex-like ops) fall back to the interpreted evaluator.
//!
//! **Vectorized path:** large OLAP fragments are lowered by the CBO
//! (`JITVectorizationRule`) to [`crate::vectorized`] batch operators that apply
//! SIMD kernels (AVX2/AVX-512 when available) over [`crate::vectorized::VectorBatch`]
//! chunks — see [`crate::executor::PhysicalPlan::VectorizedExec`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use cranelift::prelude::*;
use cranelift_codegen::Context as CodegenContext;
use cranelift_codegen::ir::BlockArg;
use cranelift_codegen::ir::MemFlagsData;
use cranelift_codegen::ir::immediates::Ieee64;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use parking_lot::Mutex;
use tracing::debug;

use crate::error::{Result, TakyonicError};
use crate::executor::{ExecutionContext, evaluate, evaluate_bool};
use crate::query::FilterOp;
use crate::schema::Record;
use crate::sql::{ArithOp, Expression, Value as SqlValue};
use crate::telemetry::EngineMetrics;

/// Cranelift SSA value (alias to avoid clashing with [`SqlValue`]).
type ClValue = cranelift::prelude::Value;

/// Native ABI for a compiled scalar: `fn(cols: *const i64, ncols: i64) -> i64`.
///
/// Column slots hold integer/bool bits. Float columns store `f64::to_bits()` in
/// the same `i64` slots and are bitcast inside the compiled function when the
/// expression tree needs `F64` arithmetic.
pub type JitScalarFn = unsafe extern "C" fn(*const i64, i64) -> i64;

/// Cranelift SIMD batch kernel: `fn(a, b, out, n)` over packed `F64X2` lanes.
///
/// Processes `n` f64 elements (tail handled scalar inside the compiled loop).
pub type JitBatchBinOpFn = unsafe extern "C" fn(*const f64, *const f64, *mut f64, i64);

/// How Takyonic SQL values map onto Cranelift IR.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JitIrType {
    /// [`SqlValue::Null`] — not materialised as a register; predicates treat as false.
    Null,
    /// [`SqlValue::Int`] → Cranelift `I64`.
    Int,
    /// [`SqlValue::Float`] → Cranelift `F64` (bits packed in `i64` slots).
    Float,
    /// [`SqlValue::Bool`] → Cranelift `I64` 0/1.
    Bool,
    /// [`SqlValue::String`] — not JIT-compilable; forces interpreter fallback.
    String,
}

impl JitIrType {
    /// Cranelift type used inside compiled functions (strings never appear).
    pub fn cranelift(self) -> Option<types::Type> {
        match self {
            Self::Int | Self::Bool => Some(types::I64),
            Self::Float => Some(types::F64),
            Self::Null | Self::String => None,
        }
    }
}

/// One finalized function kept alive by the owning [`JitCompiler`] module.
pub struct CompiledFn {
    /// Stable name inside the JIT module.
    pub name: String,
    /// Column layout: name → slot index into the `i64` buffer.
    pub columns: Vec<String>,
    /// Native entry point (valid while the compiler module lives).
    pub ptr: JitScalarFn,
    /// True when the function returns a boolean (0/1) rather than a numeric value.
    pub is_predicate: bool,
}

/// Cranelift JIT context: owns executable memory and compiled entry points.
pub struct JitCompiler {
    module: Mutex<JITModule>,
    ctx: Mutex<CodegenContext>,
    next_id: AtomicU64,
    /// Keep finalized function metadata for diagnostics / EXPLAIN.
    compiled: Mutex<Vec<CompiledFnMeta>>,
    metrics: Option<Arc<EngineMetrics>>,
}

struct CompiledFnMeta {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    columns: Vec<String>,
    #[allow(dead_code)]
    is_predicate: bool,
}

impl JitCompiler {
    /// Create a fresh Cranelift JIT module for the host ISA.
    pub fn new() -> Result<Self> {
        Self::new_with_metrics(None)
    }

    /// Same as [`Self::new`], recording compile latency into `metrics`.
    pub fn new_with_metrics(metrics: Option<Arc<EngineMetrics>>) -> Result<Self> {
        let builder = JITBuilder::new(default_libcall_names()).map_err(|e| {
            TakyonicError::Sql(format!("cranelift JIT builder failed: {e}"))
        })?;
        let module = JITModule::new(builder);
        let ctx = module.make_context();
        Ok(Self {
            module: Mutex::new(module),
            ctx: Mutex::new(ctx),
            next_id: AtomicU64::new(1),
            compiled: Mutex::new(Vec::new()),
            metrics,
        })
    }

    /// Number of functions compiled into this context.
    pub fn compiled_count(&self) -> usize {
        self.compiled.lock().len()
    }

    /// Attempt to compile `expr` as a numeric scalar (`i64` / float-bits).
    ///
    /// Returns [`None`] when the expression is not JIT-supported (caller must
    /// fall back to [`evaluate`]).
    pub fn compile_scalar(
        &self,
        expr: &Expression,
        columns: &[String],
    ) -> Result<Option<CompiledFn>> {
        if !is_jit_compilable(expr) {
            if let Some(m) = &self.metrics {
                m.record_jit_interpreter_fallback();
            }
            return Ok(None);
        }
        self.compile_inner(expr, columns, false)
    }

    /// Attempt to compile `expr` as a boolean predicate (`0` / `1`).
    pub fn compile_predicate(
        &self,
        expr: &Expression,
        columns: &[String],
    ) -> Result<Option<CompiledFn>> {
        if !is_jit_compilable(expr) {
            if let Some(m) = &self.metrics {
                m.record_jit_interpreter_fallback();
            }
            return Ok(None);
        }
        self.compile_inner(expr, columns, true)
    }

    fn compile_inner(
        &self,
        expr: &Expression,
        columns: &[String],
        is_predicate: bool,
    ) -> Result<Option<CompiledFn>> {
        let t0 = Instant::now();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let name = format!("takyonic_jit_{id}");
        let col_index: HashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.as_str(), i))
            .collect();

        let mut module = self.module.lock();
        let mut ctx = self.ctx.lock();
        module.clear_context(&mut ctx);

        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // cols*
        sig.params.push(AbiParam::new(types::I64)); // ncols
        sig.returns.push(AbiParam::new(types::I64));

        let func_id = module
            .declare_function(&name, Linkage::Export, &sig)
            .map_err(|e| TakyonicError::Sql(format!("JIT declare `{name}`: {e}")))?;
        ctx.func.signature = sig;

        {
            let mut fb_ctx = FunctionBuilderContext::new();
            let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
            let entry = b.create_block();
            b.append_block_params_for_function_params(entry);
            b.switch_to_block(entry);
            b.seal_block(entry);
            let cols_ptr = b.block_params(entry)[0];

            let codegen = match lower_expr(&mut b, expr, cols_ptr, &col_index, is_predicate) {
                Ok(v) => v,
                Err(Fallback) => {
                    // Abandon the half-built function — do not finalize an empty block.
                    drop(b);
                    module.clear_context(&mut ctx);
                    if let Some(m) = &self.metrics {
                        m.record_jit_interpreter_fallback();
                    }
                    return Ok(None);
                }
            };
            b.ins().return_(&[codegen]);
            b.finalize();
        }

        module
            .define_function(func_id, &mut ctx)
            .map_err(|e| TakyonicError::Sql(format!("JIT define `{name}`: {e}")))?;
        module.clear_context(&mut ctx);
        module
            .finalize_definitions()
            .map_err(|e| TakyonicError::Sql(format!("JIT finalize: {e}")))?;

        let code = module.get_finalized_function(func_id);
        let ptr: JitScalarFn = unsafe { std::mem::transmute(code) };

        self.compiled.lock().push(CompiledFnMeta {
            name: name.clone(),
            columns: columns.to_vec(),
            is_predicate,
        });
        debug!(%name, is_predicate, cols = columns.len(), "JIT compiled expression");
        if let Some(m) = &self.metrics {
            m.record_jit_compile(t0.elapsed());
        }

        Ok(Some(CompiledFn {
            name,
            columns: columns.to_vec(),
            ptr,
            is_predicate,
        }))
    }

    /// Compile a SIMD batch arithmetic kernel via Cranelift's `F64X2` vectors.
    ///
    /// Used by the vectorized OLAP path so expression loops run in packed
    /// machine code rather than only host Rust intrinsics.
    pub fn compile_batch_arith(&self, op: ArithOp) -> Result<JitBatchBinOpFn> {
        let t0 = Instant::now();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let name = format!("takyonic_simd_batch_{op:?}_{id}");

        let mut module = self.module.lock();
        let mut ctx = self.ctx.lock();
        module.clear_context(&mut ctx);

        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64)); // a*
        sig.params.push(AbiParam::new(types::I64)); // b*
        sig.params.push(AbiParam::new(types::I64)); // out*
        sig.params.push(AbiParam::new(types::I64)); // n
        // void return

        let func_id = module
            .declare_function(&name, Linkage::Export, &sig)
            .map_err(|e| TakyonicError::Sql(format!("JIT declare `{name}`: {e}")))?;
        ctx.func.signature = sig;

        {
            let mut fb_ctx = FunctionBuilderContext::new();
            let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
            let entry = b.create_block();
            let header = b.create_block();
            let body = b.create_block();
            let scalar = b.create_block();
            let do_one = b.create_block();
            let exit = b.create_block();

            b.append_block_params_for_function_params(entry);
            // header(i, a_ptr, b_ptr, out_ptr)
            b.append_block_param(header, types::I64);
            b.append_block_param(header, types::I64);
            b.append_block_param(header, types::I64);
            b.append_block_param(header, types::I64);

            b.switch_to_block(entry);
            let a0 = b.block_params(entry)[0];
            let b0 = b.block_params(entry)[1];
            let o0 = b.block_params(entry)[2];
            let n0 = b.block_params(entry)[3];
            let zero = b.ins().iconst(types::I64, 0);
            let one = b.ins().iconst(types::I64, 1);
            let two = b.ins().iconst(types::I64, 2);
            let eight = b.ins().iconst(types::I64, 8);
            let sixteen = b.ins().iconst(types::I64, 16);
            b.ins().jump(
                header,
                &[
                    BlockArg::from(zero),
                    BlockArg::from(a0),
                    BlockArg::from(b0),
                    BlockArg::from(o0),
                ],
            );
            b.seal_block(entry);

            b.switch_to_block(header);
            let i = b.block_params(header)[0];
            let ap = b.block_params(header)[1];
            let bp = b.block_params(header)[2];
            let op_ptr = b.block_params(header)[3];
            let i_plus_2 = b.ins().iadd(i, two);
            let can_vec = b.ins().icmp(IntCC::UnsignedLessThanOrEqual, i_plus_2, n0);
            b.ins().brif(can_vec, body, &[], scalar, &[]);

            // SIMD body: F64X2 load → op → store → header
            b.switch_to_block(body);
            let flags = MemFlagsData::trusted();
            let va = b.ins().load(types::F64X2, flags, ap, 0);
            let vb = b.ins().load(types::F64X2, flags, bp, 0);
            let vr = match op {
                ArithOp::Add => b.ins().fadd(va, vb),
                ArithOp::Sub => b.ins().fsub(va, vb),
                ArithOp::Mul => b.ins().fmul(va, vb),
                ArithOp::Div => b.ins().fdiv(va, vb),
            };
            b.ins().store(flags, vr, op_ptr, 0);
            let ap2 = b.ins().iadd(ap, sixteen);
            let bp2 = b.ins().iadd(bp, sixteen);
            let op2 = b.ins().iadd(op_ptr, sixteen);
            let i2 = b.ins().iadd(i, two);
            b.ins().jump(
                header,
                &[
                    BlockArg::from(i2),
                    BlockArg::from(ap2),
                    BlockArg::from(bp2),
                    BlockArg::from(op2),
                ],
            );
            b.seal_block(body);

            // Scalar: one lane or exit
            b.switch_to_block(scalar);
            let can_one = b.ins().icmp(IntCC::UnsignedLessThan, i, n0);
            b.ins().brif(can_one, do_one, &[], exit, &[]);
            b.seal_block(scalar);

            b.switch_to_block(do_one);
            let fa = b.ins().load(types::F64, flags, ap, 0);
            let fb = b.ins().load(types::F64, flags, bp, 0);
            let fr = match op {
                ArithOp::Add => b.ins().fadd(fa, fb),
                ArithOp::Sub => b.ins().fsub(fa, fb),
                ArithOp::Mul => b.ins().fmul(fa, fb),
                ArithOp::Div => b.ins().fdiv(fa, fb),
            };
            b.ins().store(flags, fr, op_ptr, 0);
            let ap3 = b.ins().iadd(ap, eight);
            let bp3 = b.ins().iadd(bp, eight);
            let op3 = b.ins().iadd(op_ptr, eight);
            let i3 = b.ins().iadd(i, one);
            b.ins().jump(
                header,
                &[
                    BlockArg::from(i3),
                    BlockArg::from(ap3),
                    BlockArg::from(bp3),
                    BlockArg::from(op3),
                ],
            );
            b.seal_block(do_one);

            // All predecessors of header are known (entry, body, do_one).
            b.seal_block(header);

            b.switch_to_block(exit);
            b.ins().return_(&[]);
            b.seal_block(exit);
            b.finalize();
        }

        module
            .define_function(func_id, &mut ctx)
            .map_err(|e| TakyonicError::Sql(format!("JIT SIMD define `{name}`: {e}")))?;
        module.clear_context(&mut ctx);
        module
            .finalize_definitions()
            .map_err(|e| TakyonicError::Sql(format!("JIT SIMD finalize: {e}")))?;

        let code = module.get_finalized_function(func_id);
        let ptr: JitBatchBinOpFn = unsafe { std::mem::transmute(code) };
        self.compiled.lock().push(CompiledFnMeta {
            name: name.clone(),
            columns: Vec::new(),
            is_predicate: false,
        });
        debug!(%name, ?op, "JIT compiled SIMD F64X2 batch kernel");
        if let Some(m) = &self.metrics {
            m.record_jit_compile(t0.elapsed());
        }
        Ok(ptr)
    }
}

impl Default for JitCompiler {
    fn default() -> Self {
        Self::new().expect("host Cranelift JIT available")
    }
}

/// Sentinel: expression cannot be lowered — fall back to interpreter.
struct Fallback;

/// True when every node in `expr` has a Cranelift lowering.
pub fn is_jit_compilable(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) => true,
        Expression::Literal(s) => {
            // Non-numeric strings have no I64/F64 lowering.
            !matches!(SqlValue::from_text(s), SqlValue::String(_))
        }
        // Parameters need bind-time constants; treat as non-JIT for now.
        Expression::Parameter(_) => false,
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. } => {
            is_jit_compilable(left) && is_jit_compilable(right)
        }
        Expression::InList { expr, list, .. } => {
            is_jit_compilable(expr)
                && list.iter().all(|v| {
                    matches!(
                        v,
                        SqlValue::Int(_)
                            | SqlValue::Float(_)
                            | SqlValue::Bool(_)
                            | SqlValue::Null
                    )
                })
        }
        Expression::AggregateFunction { .. }
        | Expression::InSubquery { .. }
        | Expression::Exists { .. }
        | Expression::ScalarSubquery { .. }
        | Expression::Array(_)
        | Expression::ArrayIndex { .. }
        | Expression::VectorDistance { .. }
        | Expression::Like { .. }
        | Expression::SimilarTo { .. }
        | Expression::RegexMatch { .. }
        | Expression::AtTimeZone { .. }
        | Expression::Case { .. }
        | Expression::IsNull { .. }
        | Expression::IsBoolTest { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::QuantifiedCmp { .. }
        | Expression::Coalesce(_)
        | Expression::Cast { .. }
        | Expression::NullIf { .. }
        | Expression::ScalarFunction { .. }
        | Expression::Not { .. }
        | Expression::OuterRef(_) => false,
    }
}

/// Collect column names referenced by a JIT-able expression (stable order).
pub fn collect_jit_columns(expr: &Expression) -> Vec<String> {
    let mut cols = Vec::new();
    walk_cols(expr, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn walk_cols(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        Expression::Column(c) => out.push(c.clone()),
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right }
        | Expression::Arith { left, right, .. }
        | Expression::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::SimilarTo {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::RegexMatch {
            expr: left,
            pattern: right,
            ..
        }
        | Expression::AtTimeZone {
            timestamp: left,
            time_zone: right,
        } => {
            walk_cols(left, out);
            walk_cols(right, out);
        }
        Expression::Case {
            when_then,
            else_result,
        } => {
            for (cond, result) in when_then {
                walk_cols(cond, out);
                walk_cols(result, out);
            }
            if let Some(e) = else_result {
                walk_cols(e, out);
            }
        }
        Expression::IsNull { expr, .. }
        | Expression::IsBoolTest { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Not { expr } => {
            walk_cols(expr, out);
        }
        Expression::NullIf { left, right }
        | Expression::IsDistinctFrom { left, right, .. }
        | Expression::QuantifiedCmp { left, right, .. } => {
            walk_cols(left, out);
            walk_cols(right, out);
        }
        Expression::ScalarFunction { args, .. } | Expression::AggregateFunction { args, .. } => {
            for a in args {
                walk_cols(a, out);
            }
        }
        Expression::InList { expr, .. } => walk_cols(expr, out),
        _ => {}
    }
}

/// Pack a row into the `i64` slot buffer expected by a compiled function.
pub fn pack_row(row: &Record, columns: &[String], out: &mut [i64]) {
    debug_assert_eq!(columns.len(), out.len());
    for (i, name) in columns.iter().enumerate() {
        out[i] = match row.get(name).map(SqlValue::from_text) {
            Some(SqlValue::Int(n)) => n,
            Some(SqlValue::Float(f)) => f.to_bits() as i64,
            Some(SqlValue::Bool(b)) => i64::from(b),
            Some(SqlValue::Null) | None => 0,
            Some(SqlValue::String(s)) => s.parse::<i64>().unwrap_or(0),
        };
    }
}

/// Evaluate with JIT when possible; otherwise interpret.
pub fn evaluate_jit_or_interp(
    compiled: Option<&CompiledFn>,
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    scratch: &mut Vec<i64>,
) -> Result<SqlValue> {
    evaluate_jit_or_interp_metrics(compiled, expr, row, ctx, scratch, None)
}

/// Like [`evaluate_jit_or_interp`], optionally counting JIT executions.
pub fn evaluate_jit_or_interp_metrics(
    compiled: Option<&CompiledFn>,
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    scratch: &mut Vec<i64>,
    metrics: Option<&EngineMetrics>,
) -> Result<SqlValue> {
    if let Some(cf) = compiled {
        scratch.resize(cf.columns.len(), 0);
        pack_row(row, &cf.columns, scratch);
        let raw = unsafe { (cf.ptr)(scratch.as_ptr(), scratch.len() as i64) };
        if let Some(m) = metrics {
            m.record_jit_execution();
        }
        if cf.is_predicate {
            return Ok(SqlValue::Bool(raw != 0));
        }
        return Ok(SqlValue::Int(raw));
    }
    if let Some(m) = metrics {
        m.record_jit_interpreter_fallback();
    }
    evaluate(expr, row, ctx)
}

/// Boolean evaluate with JIT fallback.
pub fn evaluate_bool_jit_or_interp(
    compiled: Option<&CompiledFn>,
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    scratch: &mut Vec<i64>,
) -> Result<bool> {
    evaluate_bool_jit_or_interp_metrics(compiled, expr, row, ctx, scratch, None)
}

/// Like [`evaluate_bool_jit_or_interp`], optionally counting JIT executions.
pub fn evaluate_bool_jit_or_interp_metrics(
    compiled: Option<&CompiledFn>,
    expr: &Expression,
    row: &Record,
    ctx: &ExecutionContext,
    scratch: &mut Vec<i64>,
    metrics: Option<&EngineMetrics>,
) -> Result<bool> {
    if let Some(cf) = compiled {
        scratch.resize(cf.columns.len(), 0);
        pack_row(row, &cf.columns, scratch);
        let raw = unsafe { (cf.ptr)(scratch.as_ptr(), scratch.len() as i64) };
        if let Some(m) = metrics {
            m.record_jit_execution();
        }
        return Ok(raw != 0);
    }
    if let Some(m) = metrics {
        m.record_jit_interpreter_fallback();
    }
    evaluate_bool(expr, row, ctx)
}

fn load_col(b: &mut FunctionBuilder<'_>, cols_ptr: ClValue, idx: usize) -> ClValue {
    let offset = (idx * std::mem::size_of::<i64>()) as i32;
    b.ins()
        .load(types::I64, MemFlagsData::trusted(), cols_ptr, offset)
}

fn lower_expr(
    b: &mut FunctionBuilder<'_>,
    expr: &Expression,
    cols_ptr: ClValue,
    cols: &HashMap<&str, usize>,
    as_bool: bool,
) -> std::result::Result<ClValue, Fallback> {
    match expr {
        Expression::Column(name) => {
            let idx = *cols.get(name.as_str()).ok_or(Fallback)?;
            let v = load_col(b, cols_ptr, idx);
            if as_bool {
                Ok(bool_from_i64(b, v))
            } else {
                Ok(v)
            }
        }
        Expression::Literal(s) => {
            let v = SqlValue::from_text(s);
            literal_to_ir(b, &v, as_bool)
        }
        Expression::Parameter(_) => Err(Fallback),
        Expression::BinaryOp { left, op, right } => {
            let l = lower_expr(b, left, cols_ptr, cols, false)?;
            let r = lower_expr(b, right, cols_ptr, cols, false)?;
            let cc = match op {
                FilterOp::Eq => IntCC::Equal,
                FilterOp::Ne => IntCC::NotEqual,
                FilterOp::Gt => IntCC::SignedGreaterThan,
                FilterOp::Gte => IntCC::SignedGreaterThanOrEqual,
                FilterOp::Lt => IntCC::SignedLessThan,
                FilterOp::Lte => IntCC::SignedLessThanOrEqual,
            };
            let cmp = b.ins().icmp(cc, l, r);
            Ok(select_bool(b, cmp))
        }
        Expression::And { left, right } => {
            let l = lower_expr(b, left, cols_ptr, cols, true)?;
            let r = lower_expr(b, right, cols_ptr, cols, true)?;
            let land = b.ins().band(l, r);
            let zero = b.ins().iconst(types::I64, 0);
            let cmp = b.ins().icmp(IntCC::NotEqual, land, zero);
            Ok(select_bool(b, cmp))
        }
        Expression::Or { left, right } => {
            let l = lower_expr(b, left, cols_ptr, cols, true)?;
            let r = lower_expr(b, right, cols_ptr, cols, true)?;
            let lor = b.ins().bor(l, r);
            let zero = b.ins().iconst(types::I64, 0);
            let cmp = b.ins().icmp(IntCC::NotEqual, lor, zero);
            Ok(select_bool(b, cmp))
        }
        Expression::Arith { left, op, right } => {
            if expr_needs_float(left) || expr_needs_float(right) {
                let l = lower_as_f64(b, left, cols_ptr, cols)?;
                let r = lower_as_f64(b, right, cols_ptr, cols)?;
                let f = match op {
                    ArithOp::Add => b.ins().fadd(l, r),
                    ArithOp::Sub => b.ins().fsub(l, r),
                    ArithOp::Mul => b.ins().fmul(l, r),
                    ArithOp::Div => b.ins().fdiv(l, r),
                };
                let bits = b.ins().bitcast(types::I64, MemFlagsData::new(), f);
                if as_bool {
                    Ok(bool_from_i64(b, bits))
                } else {
                    Ok(bits)
                }
            } else {
                let l = lower_expr(b, left, cols_ptr, cols, false)?;
                let r = lower_expr(b, right, cols_ptr, cols, false)?;
                let v = match op {
                    ArithOp::Add => b.ins().iadd(l, r),
                    ArithOp::Sub => b.ins().isub(l, r),
                    ArithOp::Mul => b.ins().imul(l, r),
                    ArithOp::Div => b.ins().sdiv(l, r),
                };
                if as_bool {
                    Ok(bool_from_i64(b, v))
                } else {
                    Ok(v)
                }
            }
        }
        Expression::InList {
            expr: inner,
            list,
            negated,
        } => {
            let v = lower_expr(b, inner, cols_ptr, cols, false)?;
            let mut acc = b.ins().iconst(types::I64, 0);
            for item in list {
                let lit = match item {
                    SqlValue::Int(n) => b.ins().iconst(types::I64, *n),
                    SqlValue::Bool(bv) => b.ins().iconst(types::I64, i64::from(*bv)),
                    SqlValue::Float(f) => b.ins().iconst(types::I64, f.to_bits() as i64),
                    SqlValue::Null => continue,
                    SqlValue::String(_) => return Err(Fallback),
                };
                let eq = b.ins().icmp(IntCC::Equal, v, lit);
                let one = select_bool(b, eq);
                acc = b.ins().bor(acc, one);
            }
            if *negated {
                let one = b.ins().iconst(types::I64, 1);
                acc = b.ins().bxor(acc, one);
            }
            Ok(acc)
        }
        _ => Err(Fallback),
    }
}

fn expr_needs_float(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(s) => matches!(SqlValue::from_text(s), SqlValue::Float(_)),
        Expression::Arith { left, right, .. } => {
            expr_needs_float(left) || expr_needs_float(right)
        }
        Expression::BinaryOp { left, right, .. }
        | Expression::And { left, right }
        | Expression::Or { left, right } => {
            expr_needs_float(left) || expr_needs_float(right)
        }
        _ => false,
    }
}

fn lower_as_f64(
    b: &mut FunctionBuilder<'_>,
    expr: &Expression,
    cols_ptr: ClValue,
    cols: &HashMap<&str, usize>,
) -> std::result::Result<ClValue, Fallback> {
    match expr {
        Expression::Literal(s) => match SqlValue::from_text(s) {
            SqlValue::Float(f) => Ok(b.ins().f64const(Ieee64::with_float(f))),
            SqlValue::Int(n) => {
                let i = b.ins().iconst(types::I64, n);
                Ok(b.ins().fcvt_from_sint(types::F64, i))
            }
            _ => Err(Fallback),
        },
        Expression::Column(name) => {
            let idx = *cols.get(name.as_str()).ok_or(Fallback)?;
            let bits = load_col(b, cols_ptr, idx);
            Ok(b.ins().fcvt_from_sint(types::F64, bits))
        }
        Expression::Arith { left, op, right } => {
            let l = lower_as_f64(b, left, cols_ptr, cols)?;
            let r = lower_as_f64(b, right, cols_ptr, cols)?;
            Ok(match op {
                ArithOp::Add => b.ins().fadd(l, r),
                ArithOp::Sub => b.ins().fsub(l, r),
                ArithOp::Mul => b.ins().fmul(l, r),
                ArithOp::Div => b.ins().fdiv(l, r),
            })
        }
        other => {
            let i = lower_expr(b, other, cols_ptr, cols, false)?;
            Ok(b.ins().fcvt_from_sint(types::F64, i))
        }
    }
}

fn literal_to_ir(
    b: &mut FunctionBuilder<'_>,
    v: &SqlValue,
    as_bool: bool,
) -> std::result::Result<ClValue, Fallback> {
    let i = match v {
        SqlValue::Int(n) => b.ins().iconst(types::I64, *n),
        SqlValue::Bool(bv) => b.ins().iconst(types::I64, i64::from(*bv)),
        SqlValue::Float(f) => b.ins().iconst(types::I64, f.to_bits() as i64),
        SqlValue::Null => b.ins().iconst(types::I64, 0),
        SqlValue::String(_) => return Err(Fallback),
    };
    if as_bool {
        Ok(bool_from_i64(b, i))
    } else {
        Ok(i)
    }
}

fn bool_from_i64(b: &mut FunctionBuilder<'_>, v: ClValue) -> ClValue {
    let zero = b.ins().iconst(types::I64, 0);
    let cmp = b.ins().icmp(IntCC::NotEqual, v, zero);
    select_bool(b, cmp)
}

fn select_bool(b: &mut FunctionBuilder<'_>, cmp: ClValue) -> ClValue {
    let zero = b.ins().iconst(types::I64, 0);
    let one = b.ins().iconst(types::I64, 1);
    b.ins().select(cmp, one, zero)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::FilterOp;

    #[test]
    fn jit_arith_matches_interpreter() {
        let jit = JitCompiler::new().unwrap();
        let expr = Expression::Arith {
            left: Box::new(Expression::Column("salary".into())),
            op: ArithOp::Mul,
            right: Box::new(Expression::Column("tax_rate".into())),
        };
        let cols = collect_jit_columns(&expr);
        let compiled = jit.compile_scalar(&expr, &cols).unwrap().expect("compilable");
        let row = Record::new().set("salary", "100").set("tax_rate", "3");
        let mut scratch = vec![0i64; cols.len()];
        pack_row(&row, &compiled.columns, &mut scratch);
        let got = unsafe { (compiled.ptr)(scratch.as_ptr(), scratch.len() as i64) };
        let interp = evaluate(&expr, &row, &ExecutionContext::new()).unwrap();
        assert_eq!(SqlValue::Int(got), interp);
        assert_eq!(got, 300);
    }

    #[test]
    fn jit_filter_matches_interpreter() {
        let jit = JitCompiler::new().unwrap();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".into())),
            op: FilterOp::Gt,
            right: Box::new(Expression::Literal("30".into())),
        };
        let cols = collect_jit_columns(&expr);
        let compiled = jit
            .compile_predicate(&expr, &cols)
            .unwrap()
            .expect("compilable");
        let ctx = ExecutionContext::new();
        let mut scratch = Vec::new();
        for age in [25i64, 31, 30] {
            let row = Record::new().set("age", age.to_string());
            let jit_v =
                evaluate_bool_jit_or_interp(Some(&compiled), &expr, &row, &ctx, &mut scratch)
                    .unwrap();
            let interp = evaluate_bool(&expr, &row, &ctx).unwrap();
            assert_eq!(jit_v, interp, "age={age}");
        }
    }

    #[test]
    fn string_expr_falls_back() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("name".into())),
            op: FilterOp::Eq,
            right: Box::new(Expression::Literal("Ada".into())),
        };
        assert!(!is_jit_compilable(&expr));
        let jit = JitCompiler::new().unwrap();
        let cols = collect_jit_columns(&expr);
        let compiled = jit.compile_predicate(&expr, &cols).unwrap();
        assert!(compiled.is_none());
    }

    #[test]
    fn cranelift_simd_f64x2_batch_mul_matches_scalar() {
        let jit = JitCompiler::new().unwrap();
        let kernel = jit.compile_batch_arith(ArithOp::Mul).unwrap();
        let n = 1025usize; // odd tail exercises scalar path
        let a: Vec<f64> = (0..n).map(|i| (i % 97) as f64 + 0.25).collect();
        let b: Vec<f64> = (0..n).map(|i| (i % 13) as f64 * 0.5 + 1.0).collect();
        let mut out = vec![0.0; n];
        unsafe {
            kernel(a.as_ptr(), b.as_ptr(), out.as_mut_ptr(), n as i64);
        }
        for i in 0..n {
            let expect = a[i] * b[i];
            assert!(
                (out[i] - expect).abs() < 1e-9,
                "lane {i}: {} vs {expect}",
                out[i]
            );
        }
    }
}
