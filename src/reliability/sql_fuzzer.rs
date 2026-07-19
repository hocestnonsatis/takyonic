//! Grammar-guided SQL fuzzer.

/// Seeded generator of SQL statements over the supported subset.
pub struct SqlGrammarFuzzer {
    state: u64,
    /// Original seed (for diagnostics).
    pub seed: u64,
}

impl SqlGrammarFuzzer {
    /// Create a fuzzer; `seed | 1` avoids a zero xorshift state.
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed | 1,
            seed,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn pick(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    /// Produce the next statement string.
    pub fn next_sql(&mut self) -> String {
        match self.pick(12) {
            0 => "BEGIN".into(),
            1 => "COMMIT".into(),
            2 => "ROLLBACK".into(),
            3 => {
                let id = self.pick(32) + 1;
                let bal = self.pick(1000);
                format!(
                    "INSERT INTO accounts (id, balance, owner) VALUES ({id}, {bal}, 'u{}')",
                    self.pick(8)
                )
            }
            4 => format!(
                "UPDATE accounts SET balance = {} WHERE id = {}",
                self.pick(1000),
                self.pick(32) + 1
            ),
            5 => format!("DELETE FROM accounts WHERE id = {}", self.pick(32) + 1),
            6 => {
                let id = self.pick(64) + 1;
                format!(
                    "INSERT INTO orders (id, account_id, amount) VALUES ({id}, {}, {})",
                    self.pick(32) + 1,
                    self.pick(500)
                )
            }
            7 => format!(
                "SELECT a.id, SUM(o.amount) FROM accounts a \
                 JOIN orders o ON a.id = o.account_id \
                 WHERE a.id = {} GROUP BY a.id HAVING SUM(o.amount) > 0",
                self.pick(32) + 1
            ),
            8 => format!(
                "SELECT id, balance FROM accounts WHERE id = {} ORDER BY id LIMIT {}",
                self.pick(32) + 1,
                self.pick(10) + 1
            ),
            9 => "WITH c AS (SELECT id, balance FROM accounts) SELECT id FROM c LIMIT 10".into(),
            10 => format!(
                "SELECT id FROM accounts a WHERE EXISTS \
                 (SELECT 1 FROM orders o WHERE o.account_id = a.id AND o.amount > {})",
                self.pick(100)
            ),
            _ => {
                if self.pick(2) == 0 {
                    "CREATE INDEX IF NOT EXISTS idx_owner ON accounts(owner)".into()
                } else {
                    "DROP INDEX IF EXISTS idx_owner".into()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_reproduces_sql_stream() {
        let mut a = SqlGrammarFuzzer::new(12345);
        let mut b = SqlGrammarFuzzer::new(12345);
        let sa: Vec<_> = (0..50).map(|_| a.next_sql()).collect();
        let sb: Vec<_> = (0..50).map(|_| b.next_sql()).collect();
        assert_eq!(sa, sb);
    }

    #[test]
    fn never_emits_forbidden_keywords() {
        let mut f = SqlGrammarFuzzer::new(7);
        let forbidden = [
            " OVER ",
            "MERGE ",
            "ON CONFLICT",
            "LATERAL ",
            "<->",
            "CREATE USER",
            "GRANT ",
            "REVOKE ",
        ];
        for _ in 0..500 {
            let sql = format!(" {} ", f.next_sql().to_ascii_uppercase());
            for bad in forbidden {
                assert!(!sql.contains(bad), "forbidden `{bad}` in: {sql}");
            }
        }
    }

    #[test]
    fn statements_mention_known_tables() {
        let mut f = SqlGrammarFuzzer::new(99);
        let mut hit_accounts = false;
        let mut hit_orders = false;
        for _ in 0..200 {
            let s = f.next_sql();
            if s.contains("accounts") {
                hit_accounts = true;
            }
            if s.contains("orders") {
                hit_orders = true;
            }
        }
        assert!(hit_accounts && hit_orders);
    }
}
