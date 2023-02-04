# Aapari

## Overview
Aapari is a key-value store library that is not aimed for actual use but instead studies how different database algorithms work.

It is currently implemented using linear hashing and is not thread-safe.

## Usage

Check the [db_test.go](./db/db_test.go) file for example usage.

## Roadmap

- [ ] Change from linear hashing to B+ tree
- [ ] Add support for transactions / batching
- [ ] Make it at least somewhat thread-safe
- [ ] Experiment with bloom filters
- [ ] Code is pretty messy, clean it up
- [ ] Add more tests
- [ ] Add benchmarks