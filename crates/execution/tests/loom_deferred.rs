use std::collections::VecDeque;

use loom::{
    sync::{Arc, Condvar, Mutex},
    thread,
};

#[test]
fn one_block_handoff_preserves_consensus_order_under_all_interleavings() {
    loom::model(|| {
        let queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let completed = Arc::new(Mutex::new(Vec::new()));

        let worker_queue = Arc::clone(&queue);
        let worker_completed = Arc::clone(&completed);
        let worker = thread::spawn(move || {
            for _ in 0..2 {
                let (lock, ready) = &*worker_queue;
                let mut guard = lock.lock().unwrap();
                while guard.is_empty() {
                    guard = ready.wait(guard).unwrap();
                }
                let height = guard.pop_front().unwrap();
                ready.notify_all();
                drop(guard);
                worker_completed.lock().unwrap().push(height);
            }
        });

        let producer_queue = Arc::clone(&queue);
        let producer = thread::spawn(move || {
            for height in [1_u64, 2] {
                let (lock, ready) = &*producer_queue;
                let mut guard = lock.lock().unwrap();
                while !guard.is_empty() {
                    guard = ready.wait(guard).unwrap();
                }
                guard.push_back(height);
                ready.notify_all();
            }
        });

        producer.join().unwrap();
        worker.join().unwrap();
        assert_eq!(*completed.lock().unwrap(), vec![1, 2]);
    });
}
