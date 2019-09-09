Cleanup:
    - [x] Update dependencies
    - [ ] Swap error-chain for failure crate // zlb: in progress 
    - [ ] Comb through crate for poor design patterns
    - [ ] consider replacing openssl lib with rustls or update to openssl 0.10
    - [ ] use the Default trait // zlb: is this what is referenced in [producer](./src/producer.rs) around line 100?
    - [ ] Investigate Error::clone(), why was non-exhaustive
    - [ ] Revisit: clippy::if_same_then_else
    - [ ] Ensure error codes are up to date against the protocol