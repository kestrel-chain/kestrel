use std::collections::BTreeSet;

use execution::conflicts_with_committed_writes;
use loom::{
    model,
    sync::{Arc, Mutex},
    thread,
};
use state::StateAccesses;
use types::Hash;

#[test]
fn speculative_completion_order_cannot_change_canonical_validation() {
    model(|| {
        let slots = Arc::new(Mutex::new(vec![None, None]));
        let mut workers = Vec::new();
        for index in 0..2 {
            let slots = Arc::clone(&slots);
            workers.push(thread::spawn(move || {
                let key = Hash::digest([7_u8]);
                slots.lock().unwrap()[index] = Some(StateAccesses {
                    reads: BTreeSet::from([key]),
                    writes: BTreeSet::from([key]),
                });
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        let slots = slots.lock().unwrap();
        let mut committed_writes = BTreeSet::new();
        let mut conflicts = Vec::new();
        for slot in slots.iter() {
            let accesses = slot.as_ref().unwrap();
            conflicts.push(conflicts_with_committed_writes(accesses, &committed_writes));
            committed_writes.extend(&accesses.writes);
        }
        assert_eq!(conflicts, vec![false, true]);
    });
}
