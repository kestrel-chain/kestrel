module 0x__KESTREL_PUBLISHER__::Token {
    struct Balance has key { value: u64 }

    public entry fun mint(account: &signer, value: u64) {
        move_to(account, Balance { value })
    }

    public entry fun transfer(from: address, to: address, amount: u64) acquires Balance {
        let source = borrow_global_mut<Balance>(from);
        assert!(source.value >= amount, 1);
        source.value = source.value - amount;
        let destination = borrow_global_mut<Balance>(to);
        destination.value = destination.value + amount;
    }
}
